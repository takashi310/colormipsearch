package org.janelia.colormipsearch.cmsdrivers;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterators;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.colormipsearch.api.cdmips.MIPImage;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api.cdmips.MIPsUtils;
import org.janelia.colormipsearch.api.cdsearch.ColorDepthSearchAlgorithm;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPMatchScore;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearch;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResult;
import org.janelia.colormipsearch.api.imageprocessing.ImageArray;
import org.janelia.colormipsearch.api.imageprocessing.MappingFunction;
import org.janelia.colormipsearch.utils.CachedMIPsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SparkColorMIPSearch implements ColorMIPSearchDriver, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SparkColorMIPSearch.class);

    private final ColorMIPSearch colorMIPSearch;
    private final List<String> gradientsLocations;
    private final MappingFunction<String, String> gradientVariantSuffixMapping;
    private final List<String> zgapMasksLocations;
    private final MappingFunction<String, String> zgapMaskVariantSuffixMapping;
    private transient final JavaSparkContext sparkContext;

    public SparkColorMIPSearch(String appName,
                               ColorMIPSearch colorMIPSearch,
                               List<String> gradientsLocations,
                               MappingFunction<String, String> gradientVariantSuffixMapping,
                               List<String> zgapMasksLocations,
                               MappingFunction<String, String> zgapMaskVariantSuffixMapping) {
        this.colorMIPSearch = colorMIPSearch;
        this.gradientsLocations = gradientsLocations;
        this.gradientVariantSuffixMapping = gradientVariantSuffixMapping;
        this.zgapMasksLocations = zgapMasksLocations;
        this.zgapMaskVariantSuffixMapping = zgapMaskVariantSuffixMapping;
        this.sparkContext = new JavaSparkContext(new SparkConf().setAppName(appName));
    }

    @Override
    public List<ColorMIPSearchResult> findAllColorDepthMatches(List<MIPMetadata> queryMIPS, List<MIPMetadata> targetMIPS) {
        long nTargets = targetMIPS.size();
        long nQueries = queryMIPS.size();
        LOG.info("Searching {} queries against {} targets", nQueries, nTargets);

        JavaRDD<MIPImage> targetsRDD = sparkContext.parallelize(targetMIPS)
                .filter(MIPsUtils::exists)
                .map(MIPsUtils::loadMIP);
        LOG.info("Created RDD targets and put {} items into {} partitions", nTargets, targetsRDD.getNumPartitions());

        return targetsRDD.mapPartitions(targetsItr -> queryMIPS.stream().map(queryMIP -> new Tuple2<>(queryMIP, targetsItr)).iterator(), true)
                .filter(queryTargetsPair -> MIPsUtils.exists(queryTargetsPair._1))
                .flatMap(queryTargetsPair -> {
                    MIPImage queryImage = MIPsUtils.loadMIP(queryTargetsPair._1);
                    ColorDepthSearchAlgorithm<ColorMIPMatchScore> queryColorDepthSearch = colorMIPSearch.createQueryColorDepthSearchWithDefaultThreshold(queryImage);
                    Set<String> requiredVariantTypes = queryColorDepthSearch.getRequiredTargetVariantTypes();
                    List<ColorMIPSearchResult> srsByMask = StreamSupport.stream(Spliterators.spliterator(queryTargetsPair._2, Integer.MAX_VALUE, 0), true)
                            .map(targetImage -> {
                                Map<String, Supplier<ImageArray<?>>> variantImageSuppliers = new HashMap<>();
                                if (requiredVariantTypes.contains("gradient")) {
                                    variantImageSuppliers.put("gradient", () -> {
                                        MIPImage gradientImage = CachedMIPsUtils.loadMIP(MIPsUtils.getMIPVariantInfo(
                                                MIPsUtils.getMIPMetadata(targetImage),
                                                "gradient",
                                                gradientsLocations,
                                                gradientVariantSuffixMapping));
                                        return MIPsUtils.getImageArray(gradientImage);
                                    });
                                }
                                if (requiredVariantTypes.contains("zgap")) {
                                    variantImageSuppliers.put("zgap", () -> {
                                        MIPImage targetZGapMaskImage = CachedMIPsUtils.loadMIP(MIPsUtils.getMIPVariantInfo(
                                                MIPsUtils.getMIPMetadata(targetImage),
                                                "zgap",
                                                zgapMasksLocations,
                                                zgapMaskVariantSuffixMapping));
                                        return MIPsUtils.getImageArray(targetZGapMaskImage);
                                    });
                                }
                                try {
                                    ColorMIPMatchScore colorMIPMatchScore = queryColorDepthSearch.calculateMatchingScore(
                                            MIPsUtils.getImageArray(targetImage),
                                            variantImageSuppliers);
                                    boolean isMatch = colorMIPSearch.isMatch(colorMIPMatchScore);
                                    return new ColorMIPSearchResult(
                                            MIPsUtils.getMIPMetadata(queryImage),
                                            MIPsUtils.getMIPMetadata(targetImage),
                                            colorMIPMatchScore,
                                            isMatch,
                                            false);
                                } catch (Exception e) {
                                    LOG.error("Error performing color depth search between {} and {}", queryImage, targetImage);
                                    return new ColorMIPSearchResult(
                                            MIPsUtils.getMIPMetadata(queryImage),
                                            MIPsUtils.getMIPMetadata(targetImage),
                                            ColorMIPMatchScore.NO_MATCH,
                                            false,
                                            true);
                                }
                            })
                            .filter(srByMask -> srByMask.isMatch() || srByMask.hasErrors())
                            .collect(Collectors.toList());
                    return srsByMask.iterator();
                })
                .collect();
    }

    @Override
    public void terminate() {
        sparkContext.close();
    }

}
