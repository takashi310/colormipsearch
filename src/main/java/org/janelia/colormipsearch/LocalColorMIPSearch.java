package org.janelia.colormipsearch;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Streams;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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

        LOG.info("Load {} libraries", nlibraries);
        List<Pair<MIPImage, MIPImage>> libraryImagesWithGradients = libraryMIPS.stream().parallel()
                .filter(MIPInfo::exists)
                .map(mip -> ImmutablePair.of(
                        MIPsUtils.loadMIP(mip),
                        MIPsUtils.loadMIP(MIPsUtils.getGradientMIPInfo(mip, gradientMasksPath)))
                )
                .collect(Collectors.toList());
        LOG.info("Loaded {} libraries in memory in {}s", nlibraries, (System.currentTimeMillis()-startTime)/1000);

        List<CompletableFuture<List<ColorMIPSearchResult>>> allColorDepthSearches = Streams.zip(
                IntStream.range(0, maskMIPS.size()).boxed(),
                maskMIPS.stream().filter(MIPInfo::exists),
                (mIndex, maskMIP) -> submitMaskSearches(mIndex + 1, maskMIP, libraryImagesWithGradients))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        LOG.info("Submitted {} color depth searches for {} masks with {} libraries in {}s",
                allColorDepthSearches.size(), maskMIPS.size(), libraryMIPS.size(), (System.currentTimeMillis()-startTime)/1000);

        List<ColorMIPSearchResult> allSearchResults = CompletableFuture.allOf(allColorDepthSearches.toArray(new CompletableFuture<?>[0]))
                .thenApply(ignoredVoidResult -> allColorDepthSearches.stream()
                        .flatMap(searchComputation -> searchComputation.join().stream())
                        .collect(Collectors.toList()))
                .join();

        LOG.info("Finished all color depth searches {} masks with {} libraries in {}s", maskMIPS.size(), libraryMIPS.size(), (System.currentTimeMillis()-startTime) / 1000);
        return allSearchResults;
    }

    private List<CompletableFuture<List<ColorMIPSearchResult>>> submitMaskSearches(int mIndex, MIPInfo maskMIP, List<Pair<MIPImage, MIPImage>> libraryImages) {
        MIPImage maskImage = MIPsUtils.loadMIP(maskMIP); // load image
        MIPImage maskGradientImage = MIPsUtils.loadMIP(MIPsUtils.getGradientMIPInfo(maskMIP, gradientMasksPath)); // load gradient
        List<CompletableFuture<List<ColorMIPSearchResult>>> cdsComputations = Utils.partitionList(libraryImages, libraryPartitionSize).stream()
                .map(librariesPartition -> {
                    Supplier<List<ColorMIPSearchResult>> searchResultSupplier = () -> {
                        LOG.info("Compare {} (mask # {}) with {} out of {} libraries", maskMIP, mIndex, librariesPartition.size(), libraryImages.size());
                        long startTime = System.currentTimeMillis();
                        List<ColorMIPSearchResult> srs = librariesPartition.stream()
                                .map(libraryWithGradient -> {
                                    MIPImage libraryImage = libraryWithGradient.getLeft();
                                    MIPImage libraryGradientImage = libraryWithGradient.getRight();
                                    ColorMIPSearchResult sr = runImageComparison(libraryImage, maskImage);
                                    if (sr.isError()) {
                                        LOG.warn("Errors encountered comparing {} with {}", libraryImage, maskImage);
                                    } else {
                                        applyGradientAreaAdjustment(sr, libraryImage, libraryGradientImage, maskImage, maskGradientImage);
                                    }
                                    return sr;
                                })
                                .filter(ColorMIPSearchResult::isMatch)
                                .collect(Collectors.toList());
                        LOG.info("Found {} results with matches comparing {} (mask # {}) with {} libraries in {}ms", srs.size(), maskMIP, mIndex, librariesPartition.size(), System.currentTimeMillis()-startTime);
                        return srs;
                    };
                    return CompletableFuture.supplyAsync(searchResultSupplier, cdsExecutor);
                })
                .collect(Collectors.toList());
        LOG.info("Submitted {} color depth searches for {} (mask # {}) with {} libraries", cdsComputations.size(), maskMIP, mIndex, libraryImages.size());
        return cdsComputations;
    }

}
