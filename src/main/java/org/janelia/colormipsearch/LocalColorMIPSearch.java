package org.janelia.colormipsearch;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.google.common.collect.Streams;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform color depth mask search in the current process.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
class LocalColorMIPSearch extends ColorMIPSearch {

    private static final Logger LOG = LoggerFactory.getLogger(LocalColorMIPSearch.class);

    private final Executor cdsExecutor;
    private final int libraryPartitionSize;

    LocalColorMIPSearch(String gradientMasksPath,
                        Integer dataThreshold,
                        Integer maskThreshold,
                        Double pixColorFluctuation,
                        Integer xyShift,
                        int negativeRadius,
                        boolean mirrorMask,
                        Double pctPositivePixels,
                        int libraryPartitionSize,
                        Executor cdsExecutor) {
        super(gradientMasksPath, dataThreshold, maskThreshold, pixColorFluctuation, xyShift, negativeRadius, mirrorMask, pctPositivePixels);
        this.libraryPartitionSize = libraryPartitionSize > 0 ? libraryPartitionSize : 1;
        this.cdsExecutor = cdsExecutor;
    }

    @Override
    List<ColorMIPSearchResult> findAllColorDepthMatches(List<MIPInfo> maskMIPS, List<MIPInfo> libraryMIPS) {
        long startTime = System.currentTimeMillis();
        int nmasks = maskMIPS.size();
        int nlibraries = libraryMIPS.size();

        LOG.info("Searching {} masks against {} libraries", nmasks, nlibraries);

        List<CompletableFuture<List<ColorMIPSearchResult>>> allColorDepthSearches = Streams.zip(
                LongStream.range(0, maskMIPS.size()).boxed(),
                maskMIPS.stream().filter(MIPInfo::exists),
                (mIndex, maskMIP) -> submitMaskSearches(mIndex + 1, maskMIP, libraryMIPS))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        LOG.info("Submitted all {} color depth searches for {} masks with {} libraries in {}s",
                allColorDepthSearches.size(), maskMIPS.size(), libraryMIPS.size(), (System.currentTimeMillis()-startTime)/1000);

        List<ColorMIPSearchResult> allSearchResults = CompletableFuture.allOf(allColorDepthSearches.toArray(new CompletableFuture<?>[0]))
                .thenApply(ignoredVoidResult -> allColorDepthSearches.stream()
                        .flatMap(searchComputation -> searchComputation.join().stream())
                        .collect(Collectors.toList()))
                .join();

        LOG.info("Finished all color depth searches {} masks with {} libraries in {}s", maskMIPS.size(), libraryMIPS.size(), (System.currentTimeMillis()-startTime) / 1000);
        return allSearchResults;
    }

    private List<CompletableFuture<List<ColorMIPSearchResult>>> submitMaskSearches(long mIndex, MIPInfo maskMIP, List<MIPInfo> libraryMIPs) {
        MIPImage maskImage = MIPsUtils.loadMIP(maskMIP); // load image - no caching for the mask
        List<CompletableFuture<List<ColorMIPSearchResult>>> cdsComputations = Utils.partitionList(libraryMIPs, libraryPartitionSize).stream()
                .map(libraryMIPsPartition -> {
                    Supplier<List<ColorMIPSearchResult>> searchResultSupplier = () -> {
                        LOG.debug("Compare mask# {} - {} with {} out of {} libraries", mIndex, maskMIP, libraryMIPsPartition.size(), libraryMIPs.size());
                        long startTime = System.currentTimeMillis();
                        List<ColorMIPSearchResult> srs = libraryMIPsPartition.stream()
                                .map(libraryMIP -> {
                                    MIPImage libraryImage = CachedMIPsUtils.loadMIP(libraryMIP);
                                    ColorMIPSearchResult sr = runImageComparison(libraryImage, maskImage);
                                    if (sr.isMatch() && StringUtils.isNotBlank(gradientMasksPath)) {
                                        // try to load gradients
                                        MIPImage libraryGradientImage = CachedMIPsUtils.loadMIP(MIPsUtils.getGradientMIPInfo(libraryMIP, gradientMasksPath));
                                        MIPImage maskGradientImage = CachedMIPsUtils.loadMIP(MIPsUtils.getGradientMIPInfo(maskMIP, gradientMasksPath));
                                        applyGradientAreaAdjustment(sr, libraryImage, libraryGradientImage, maskImage, maskGradientImage);
                                    } else if (sr.isError()) {
                                        LOG.warn("Errors encountered comparing {} with {}", libraryImage, maskImage);
                                    }
                                    return sr;
                                })
                                .filter(ColorMIPSearchResult::isMatch)
                                .collect(Collectors.toList());
                        LOG.info("Found {} results with matches comparing mask# {} - {} with {} out of {} libraries in {}ms", srs.size(), mIndex, maskMIP, libraryMIPsPartition.size(), libraryMIPs.size(), System.currentTimeMillis()-startTime);
                        return srs;
                    };
                    return CompletableFuture.supplyAsync(searchResultSupplier, cdsExecutor);
                })
                .collect(Collectors.toList());
        LOG.info("Submitted {} partitioned color depth searches with {} libraries for mask# {} - {}",
                cdsComputations.size(), libraryMIPs.size(), mIndex, maskMIP);
        return cdsComputations;
    }

}
