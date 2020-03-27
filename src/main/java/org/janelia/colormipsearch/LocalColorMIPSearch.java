package org.janelia.colormipsearch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Streams;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
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
                        String outputPath,
                        Integer dataThreshold,
                        Integer maskThreshold,
                        Double pixColorFluctuation,
                        Integer xyShift,
                        int negativeRadius,
                        boolean mirrorMask,
                        Double pctPositivePixels,
                        int libraryPartitionSize,
                        Executor cdsExecutor) {
        super(gradientMasksPath, outputPath, dataThreshold, maskThreshold, pixColorFluctuation, xyShift, negativeRadius, mirrorMask, pctPositivePixels);
        this.libraryPartitionSize = libraryPartitionSize > 0 ? libraryPartitionSize : 1;
        this.cdsExecutor = cdsExecutor;
    }

    @Override
    void compareEveryMaskWithEveryLibrary(List<MIPInfo> maskMIPS, List<MIPInfo> libraryMIPS) {
        RetryPolicy<List<ColorMIPSearchResult>> retryPolicy = new RetryPolicy<List<ColorMIPSearchResult>>()
                .handle(IllegalStateException.class)
                .withDelay(Duration.ofMillis(500))
                .withMaxRetries(20);

        List<ColorMIPSearchResult> allSearchResults = findAllColorDepthMatches(maskMIPS, libraryMIPS);

        LOG.info("Group {} results by mask", allSearchResults.size());
        Map<String, List<ColorMIPSearchResult>> srsByMasks = allSearchResults.stream()
                .collect(Collectors.groupingBy(ColorMIPSearchResult::getMaskId, Collectors.toList()));

        LOG.info("Write {} results by mask", srsByMasks.size());
        srsByMasks.forEach((maskId, srsForCurrentMask) -> Failsafe.with(retryPolicy).run(() -> writeSearchResults(maskId, srsForCurrentMask.stream().map(ColorMIPSearchResult::perMaskMetadata).collect(Collectors.toList()))));

        LOG.info("Group {} results by library", allSearchResults.size());
        Map<String, List<ColorMIPSearchResult>> srsByLibrary = allSearchResults.stream()
                .collect(Collectors.groupingBy(ColorMIPSearchResult::getLibraryId, Collectors.toList()));

        LOG.info("Write {} results by library", srsByLibrary.size());
        srsByLibrary.forEach((libraryId, srsForCurrentLibrary) -> Failsafe.with(retryPolicy).run(() -> writeSearchResults(libraryId, srsForCurrentLibrary.stream().map(ColorMIPSearchResult::perMaskMetadata).collect(Collectors.toList()))));

        LOG.info("Finished writing {} results by {} masks and by {} libraries", allSearchResults.size(), srsByMasks.size(), srsByLibrary.size());
    }

    List<ColorMIPSearchResult> findAllColorDepthMatches(List<MIPInfo> maskMIPS, List<MIPInfo> libraryMIPS) {
        long startTime = System.currentTimeMillis();
        int nmasks = maskMIPS.size();
        int nlibraries = libraryMIPS.size();

        LOG.info("Searching {} masks against {} libraries", nmasks, nlibraries);

        LOG.info("Load {} libraries", nlibraries);
        List<Pair<MIPImage, MIPImage>> libraryImagesWithGradients = libraryMIPS.stream().parallel()
                .filter(MIPInfo::exists)
                .map(mip -> ImmutablePair.of(loadMIP(mip), loadGradientMIP(mip)))
                .collect(Collectors.toList());
        LOG.info("Loaded {} libraries in memory in {}s", nlibraries, (System.currentTimeMillis()-startTime)/1000);

        List<CompletableFuture<List<ColorMIPSearchResult>>> allColorDepthSearches = Streams.zip(
                IntStream.range(0, maskMIPS.size()).boxed(),
                maskMIPS.stream().filter(MIPInfo::exists),
                (mIndex, maskMIP) -> {
                    LOG.info("Compare {} with {} (mask # {}) libraries", maskMIP, mIndex+1, nlibraries);
                    return submitMaskSearches(mIndex, maskMIP, libraryImagesWithGradients);
                })
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
        MIPImage maskImage = loadMIP(maskMIP); // load image
        MIPImage maskGradientImage = loadGradientMIP(maskMIP); // load gradient
        List<CompletableFuture<List<ColorMIPSearchResult>>> cdsComputations = partitionList(libraryImages).stream()
                .map(librariesPartition -> {
                    Supplier<List<ColorMIPSearchResult>> searchResultSupplier = () -> {
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

    private <T> List<List<T>> partitionList(List<T> l) {
        BiFunction<Pair<List<List<T>>, List<T>>, T, Pair<List<List<T>>, List<T>>> partitionAcumulator = (partitionResult, s) -> {
            List<T> currentPartition;
            if (partitionResult.getRight().size() == libraryPartitionSize) {
                currentPartition = new ArrayList<>();
            } else {
                currentPartition = partitionResult.getRight();
            }
            currentPartition.add(s);
            if (currentPartition.size() == 1) {
                partitionResult.getLeft().add(currentPartition);
            }
            return ImmutablePair.of(partitionResult.getLeft(), currentPartition);
        };
        return l.stream().reduce(
                ImmutablePair.of(new ArrayList<>(), new ArrayList<>()),
                partitionAcumulator,
                (r1, r2) -> r2.getLeft().stream().flatMap(p -> p.stream())
                        .map(s -> partitionAcumulator.apply(r1, s))
                        .reduce((first, second) -> second)
                        .orElse(r1)).getLeft()
                ;
    }

}
