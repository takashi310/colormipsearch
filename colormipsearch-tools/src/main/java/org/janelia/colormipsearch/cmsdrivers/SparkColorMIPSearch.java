package org.janelia.colormipsearch.cmsdrivers;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
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
        List<ColorMIPSearchResult> cdsResults = queryMIPS.stream().parallel()
                .filter(MIPsUtils::exists)
                .flatMap(query -> {
                    LOG.info("Compare {} with {} targets", query, nTargets);
                    MIPImage queryImage = MIPsUtils.loadMIP(query);
                    ColorDepthSearchAlgorithm<ColorMIPMatchScore> queryColorDepthSearch = colorMIPSearch.createQueryColorDepthSearchWithDefaultThreshold(queryImage);
                    Set<String> requiredVariantTypes = queryColorDepthSearch.getRequiredTargetVariantTypes();
                    List<ColorMIPSearchResult> queryResults = sparkContext.parallelize(targetMIPS)
                            .filter(MIPsUtils::exists)
                            .map(MIPsUtils::loadMIP)
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
                                    LOG.debug("Compare query {} with target {}", queryImage, targetImage);
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
                                    LOG.error("Error performing color depth search between {} and {}", queryImage, targetImage, e);
                                    return new ColorMIPSearchResult(
                                            MIPsUtils.getMIPMetadata(queryImage),
                                            MIPsUtils.getMIPMetadata(targetImage),
                                            ColorMIPMatchScore.NO_MATCH,
                                            false,
                                            true);
                                }
                            })
                            .collect();
                    LOG.info("Found {} results between {} and {} targets", queryResults.size(), query, nTargets);
                    return queryResults.stream();
                })
                .collect(Collectors.toList());
        LOG.info("Found {} cds results", cdsResults.size());
        return cdsResults;
    }

    @Override
    public void terminate() {
        sparkContext.close();
    }

}
