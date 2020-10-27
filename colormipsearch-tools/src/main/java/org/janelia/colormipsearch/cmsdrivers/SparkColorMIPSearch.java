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
        LOG.info("Searching {} queries against {} targets", queryMIPS.size(), targetMIPS.size());

        long nTargets = targetMIPS.size();
        long nQueries = queryMIPS.size();

        JavaRDD<MIPImage> targetsRDD = sparkContext.parallelize(targetMIPS)
                .filter(MIPsUtils::exists)
                .map(MIPsUtils::loadMIP);
        LOG.info("Created RDD targets and put {} items into {} partitions", nTargets, targetsRDD.getNumPartitions());

        JavaRDD<MIPMetadata> queriesRDD = sparkContext.parallelize(queryMIPS)
                .filter(MIPsUtils::exists);
        LOG.info("Created RDD queries and put {} items into {} partitions", nQueries, queriesRDD.getNumPartitions());

        JavaPairRDD<MIPMetadata, MIPImage> queryTargetPairsRDD = queriesRDD.cartesian(targetsRDD);
        LOG.info("Created {} query target pairs in {} partitions", nQueries * nTargets, queryTargetPairsRDD.getNumPartitions());

        JavaPairRDD<MIPMetadata, List<ColorMIPSearchResult>> allSearchResultsPartitionedByMaskMIP = queryTargetPairsRDD
                .groupBy(tq -> tq._1) // group by query
                .mapPartitions(qtItr -> StreamSupport.stream(Spliterators.spliterator(qtItr, Integer.MAX_VALUE, 0), false)
                        .map(mls -> {
                            MIPImage queryImage = MIPsUtils.loadMIP(mls._1);
                            ColorDepthSearchAlgorithm<ColorMIPMatchScore> queryColorDepthSearch = colorMIPSearch.createQueryColorDepthSearch(queryImage, null);
                            Set<String> requiredVariantTypes = queryColorDepthSearch.getRequiredTargetVariantTypes();
                            List<ColorMIPSearchResult> srsByMask = StreamSupport.stream(mls._2.spliterator(), false)
                                    .map(queryTargetPair -> {
                                        MIPImage targetImage = queryTargetPair._2;
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
                                    })
                                    .filter(srByMask -> srByMask.isMatch() || srByMask.hasErrors())
                                    .sorted(colorMIPSearch.getColorMIPSearchResultComparator())
                                    .collect(Collectors.toList());
                            return new Tuple2<>(mls._1, srsByMask);
                        })
                        .iterator())
                .mapToPair(p -> p);
        LOG.info("Created RDD search results for  all {} query target pairs in all {} partitions", nQueries * nTargets, allSearchResultsPartitionedByMaskMIP.getNumPartitions());

        // write results for each mask
        return allSearchResultsPartitionedByMaskMIP.flatMapToPair(srByMask -> srByMask._2.stream().map(sr -> new Tuple2<>(srByMask._1, sr)).iterator())
                .map(mipWithSr -> mipWithSr._2)
                .collect();
    }

    @Override
    public void terminate() {
        sparkContext.close();
    }

}
