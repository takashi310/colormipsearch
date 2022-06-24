package org.janelia.colormipsearch.cmd_v2.cmsdrivers;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.google.common.collect.Streams;

import org.janelia.colormipsearch.api_v2.cdmips.MIPImage;
import org.janelia.colormipsearch.api_v2.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api_v2.cdmips.MIPsUtils;
import org.janelia.colormipsearch.api_v2.cdsearch.ColorDepthSearchAlgorithm;
import org.janelia.colormipsearch.api_v2.cdsearch.ColorMIPMatchScore;
import org.janelia.colormipsearch.api_v2.cdsearch.ColorMIPSearch;
import org.janelia.colormipsearch.api_v2.cdsearch.ColorMIPSearchResult;
import org.janelia.colormipsearch.imageprocessing.ImageArray;
import org.janelia.colormipsearch.imageprocessing.MappingFunction;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.janelia.colormipsearch.cmd_v2.CachedMIPsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform color depth mask search in the current process.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class LocalColorMIPSearch implements ColorMIPSearchDriver {

    private static final Logger LOG = LoggerFactory.getLogger(LocalColorMIPSearch.class);
    private static final long _1M = 1024 * 1024;

    private final ColorMIPSearch colorMIPSearch;
    private final Executor cdsExecutor;
    private final int localProcessingPartitionSize;
    private final List<String> gradientsLocations;
    private final MappingFunction<String, String> gradientVariantSuffixMapping;
    private final List<String> zgapMasksLocations;
    private final MappingFunction<String, String> zgapMaskVariantSuffixMapping;

    public LocalColorMIPSearch(ColorMIPSearch colorMIPSearch,
                               int localProcessingPartitionSize,
                               List<String> gradientsLocations,
                               MappingFunction<String, String> gradientVariantSuffixMapping,
                               List<String> zgapMasksLocations,
                               MappingFunction<String, String> zgapMaskVariantSuffixMapping,
                               Executor cdsExecutor) {
        this.colorMIPSearch = colorMIPSearch;
        this.localProcessingPartitionSize = localProcessingPartitionSize > 0 ? localProcessingPartitionSize : 1;
        this.gradientsLocations = gradientsLocations;
        this.gradientVariantSuffixMapping = gradientVariantSuffixMapping;
        this.zgapMasksLocations = zgapMasksLocations;
        this.zgapMaskVariantSuffixMapping = zgapMaskVariantSuffixMapping;
        this.cdsExecutor = cdsExecutor;
    }

    @Override
    public List<ColorMIPSearchResult> findAllColorDepthMatches(List<MIPMetadata> queryMIPS, List<MIPMetadata> targetMIPS) {
        long startTime = System.currentTimeMillis();
        int nQueries = queryMIPS.size();
        int nTargets = targetMIPS.size();

        LOG.info("Searching {} masks against {} targets", nQueries, nTargets);

        List<CompletableFuture<List<ColorMIPSearchResult>>> allColorDepthSearches = Streams.zip(
                LongStream.range(0, queryMIPS.size()).boxed(),
                queryMIPS.stream().filter(MIPsUtils::exists),
                (mIndex, maskMIP) -> submitMaskSearches(mIndex + 1, maskMIP, targetMIPS))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        LOG.info("Submitted all {} color depth searches for {} masks with {} targets in {}s - memory usage {}M",
                allColorDepthSearches.size(), queryMIPS.size(), targetMIPS.size(),
                (System.currentTimeMillis() - startTime) / 1000.,
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1);

        List<ColorMIPSearchResult> allSearchResults = CompletableFuture.allOf(allColorDepthSearches.toArray(new CompletableFuture<?>[0]))
                .thenApply(ignoredVoidResult -> allColorDepthSearches.stream()
                        .flatMap(searchComputation -> searchComputation.join().stream())
                        .collect(Collectors.toList()))
                .join();

        LOG.info("Finished all color depth searches {} masks with {} targets in {}s - memory usage {}M",
                queryMIPS.size(), targetMIPS.size(), (System.currentTimeMillis() - startTime) / 1000.,
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1);
        return allSearchResults;
    }

    private List<CompletableFuture<List<ColorMIPSearchResult>>> submitMaskSearches(long mIndex,
                                                                                   MIPMetadata queryMIP,
                                                                                   List<MIPMetadata> targetMIPs) {
        MIPImage queryImage = MIPsUtils.loadMIP(queryMIP); // load image - no caching for the mask
        if (queryImage == null) {
            return Collections.singletonList(
                    CompletableFuture.completedFuture(Collections.emptyList())
            );
        }
        ColorDepthSearchAlgorithm<ColorMIPMatchScore> queryColorDepthSearch = colorMIPSearch.createQueryColorDepthSearchWithDefaultThreshold(queryImage);
        if (queryColorDepthSearch.getQuerySize() == 0) {
            LOG.info("No computation created for {} because it is empty", queryMIP);
            return Collections.emptyList();
        }
        Set<String> requiredVariantTypes = queryColorDepthSearch.getRequiredTargetVariantTypes();
        List<CompletableFuture<List<ColorMIPSearchResult>>> cdsComputations = ItemsHandling.partitionCollection(targetMIPs, localProcessingPartitionSize).stream()
                .map(targetMIPsPartition -> {
                    Supplier<List<ColorMIPSearchResult>> searchResultSupplier = () -> {
                        LOG.debug("Compare query# {} - {} with {} out of {} targets", mIndex, queryMIP, targetMIPsPartition.size(), targetMIPs.size());
                        long startTime = System.currentTimeMillis();
                        List<ColorMIPSearchResult> srs = targetMIPsPartition.stream()
                                .map(CachedMIPsUtils::loadMIP)
                                .filter(mip -> mip != null)
                                .map(targetImage -> {
                                    try {
                                        Map<String, Supplier<ImageArray<?>>> variantImageSuppliers = new HashMap<>();
                                        if (requiredVariantTypes.contains("gradient")) {
                                            variantImageSuppliers.put("gradient", () -> {
                                                MIPImage gradientImage = CachedMIPsUtils.loadMIP(MIPsUtils.getMIPVariantInfo(
                                                        targetImage.getMipInfo(),
                                                        "gradient",
                                                        gradientsLocations,
                                                        gradientVariantSuffixMapping,
                                                        null));
                                                return MIPsUtils.getImageArray(gradientImage);
                                            });
                                        }
                                        if (requiredVariantTypes.contains("zgap")) {
                                            variantImageSuppliers.put("zgap", () -> {
                                                MIPImage libraryZGapMaskImage = CachedMIPsUtils.loadMIP(MIPsUtils.getMIPVariantInfo(
                                                        targetImage.getMipInfo(),
                                                        "zgap",
                                                        zgapMasksLocations,
                                                        zgapMaskVariantSuffixMapping,
                                                        null));
                                                return MIPsUtils.getImageArray(libraryZGapMaskImage);
                                            });
                                        }
                                        ColorMIPMatchScore colorMIPMatchScore = queryColorDepthSearch.calculateMatchingScore(
                                                MIPsUtils.getImageArray(targetImage),
                                                variantImageSuppliers);
                                        boolean isMatch = colorMIPSearch.isMatch(colorMIPMatchScore);
                                        return new ColorMIPSearchResult(
                                                queryMIP,
                                                targetImage.getMipInfo(),
                                                colorMIPMatchScore,
                                                isMatch,
                                                false);
                                    } catch (Throwable e) {
                                        LOG.warn("Error comparing mask {} with {}", queryMIP,  targetImage, e);
                                        return new ColorMIPSearchResult(
                                                queryMIP,
                                                targetImage.getMipInfo(),
                                                ColorMIPMatchScore.NO_MATCH,
                                                false,
                                                true);
                                    }
                                })
                                .filter(ColorMIPSearchResult::isMatch)
                                .collect(Collectors.toList());
                        LOG.info("Found {} matches comparing mask# {} - {} with {} out of {} libraries in {}ms",
                                srs.size(), mIndex, queryMIP, targetMIPsPartition.size(), targetMIPs.size(), System.currentTimeMillis() - startTime);
                        return srs;
                    };
                    return CompletableFuture.supplyAsync(searchResultSupplier, cdsExecutor);
                })
                .collect(Collectors.toList());
        LOG.info("Submitted {} partitioned color depth searches with {} libraries for mask# {} - {}",
                cdsComputations.size(), targetMIPs.size(), mIndex, queryMIP);
        return cdsComputations;
    }

    @Override
    public void terminate() {
        // nothing to do here
    }
}
