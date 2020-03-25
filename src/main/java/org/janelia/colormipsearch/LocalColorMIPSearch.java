package org.janelia.colormipsearch;

import java.io.File;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

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

    LocalColorMIPSearch(String gradientMasksPath,
                        String outputPath,
                        Integer dataThreshold,
                        Integer maskThreshold,
                        Double pixColorFluctuation,
                        Integer xyShift,
                        int negativeRadius,
                        boolean mirrorMask,
                        Double pctPositivePixels,
                        Executor cdsExecutor) {
        super(gradientMasksPath, outputPath, dataThreshold, maskThreshold, pixColorFluctuation, xyShift, negativeRadius, mirrorMask, pctPositivePixels);
        this.cdsExecutor = cdsExecutor;
    }

    @Override
    void compareEveryMaskWithEveryLibrary(List<MIPInfo> maskMIPS, List<MIPInfo> libraryMIPS) {
        long startTime = System.currentTimeMillis();
        int nmasks = maskMIPS.size();
        int nlibraries = libraryMIPS.size();

        LOG.info("Searching {} masks against {} libraries", nmasks, nlibraries);

        LOG.info("Load {} masks", nmasks);
        List<Pair<MIPImage, MIPImage>> masksMIPSWithImages = maskMIPS.stream().parallel()
                .filter(mip -> new File(mip.imagePath).exists())
                .map(mip -> ImmutablePair.of(loadMIP(mip), loadGradientMIP(mip)))
                .collect(Collectors.toList());
        LOG.info("Loaded {} masks in memory", nmasks);

        LOG.info("Compute and save search results by library");

        RetryPolicy<List<ColorMIPSearchResult>> retryPolicy = new RetryPolicy<List<ColorMIPSearchResult>>()
                .handle(IllegalStateException.class)
                .withDelay(Duration.ofMillis(500))
                .withMaxRetries(20);

        List<ColorMIPSearchResult> allSearchResults = libraryMIPS.stream()
                .filter(libraryMIP -> new File(libraryMIP.imagePath).exists())
                .map(libraryMIP -> {
                    long libraryStartTime = System.currentTimeMillis();
                    LOG.info("Compare {} with {} masks", libraryMIP, nmasks);
                    List<CompletableFuture<ColorMIPSearchResult>> librarySearches = submitLibrarySearches(libraryMIP, masksMIPSWithImages);
                    return CompletableFuture.allOf(librarySearches.toArray(new CompletableFuture<?>[0]))
                            .thenApply(ignoredVoidResult -> librarySearches.stream()
                                    .map(searchComputation -> searchComputation.join())
                                    .filter(ColorMIPSearchResult::isMatch)
                                    .collect(Collectors.toList()))
                            .thenCompose(matchingResults -> {
                                LOG.info("Found {} search results comparing {} masks with {} in {}s", matchingResults.size(), nmasks, libraryMIP, (System.currentTimeMillis() - libraryStartTime) / 1000);
                                return Failsafe.with(retryPolicy).getAsync(() -> {
                                    writeSearchResults(libraryMIP.id, matchingResults.stream().map(ColorMIPSearchResult::perLibraryMetadata).collect(Collectors.toList()));
                                    return matchingResults;
                                });
                            })
                            .join()
                            ;
                })
                .flatMap(Collection::stream)
                .collect(Collectors.toList())
                ;
        LOG.info("Finished comparing {} masks with {} libraries in {}s", maskMIPS.size(), libraryMIPS.size(), (System.currentTimeMillis() - startTime) / 1000);

        Map<String, List<ColorMIPSearchResult>> srsByMasks = allSearchResults.stream()
                .collect(Collectors.groupingBy(ColorMIPSearchResult::getMaskId, Collectors.toList()));

        LOG.info("Write {} results by mask", srsByMasks.size());
        srsByMasks.forEach((maskId, srsForCurrentMask) -> Failsafe.with(retryPolicy).run(() -> writeSearchResults(maskId, srsForCurrentMask.stream().map(ColorMIPSearchResult::perMaskMetadata).collect(Collectors.toList()))));

        LOG.info("Finished writing results after comparing {} masks with {} libraries in {}s", maskMIPS.size(), libraryMIPS.size(), (System.currentTimeMillis() - startTime) / 1000);
    }

    List<ColorMIPSearchResult> findAllColorDepthMatches(List<MIPInfo> maskMIPS, List<MIPInfo> libraryMIPS) {
        long startTime = System.currentTimeMillis();
        int nmasks = maskMIPS.size();
        int nlibraries = libraryMIPS.size();

        LOG.info("Searching {} masks against {} libraries", nmasks, nlibraries);

        LOG.info("Load {} masks", nmasks);
        List<Pair<MIPImage, MIPImage>> masksMIPSWithImages = maskMIPS.stream().parallel()
                .filter(mip -> mip.exists())
                .map(mip -> ImmutablePair.of(loadMIP(mip), loadGradientMIP(mip)))
                .collect(Collectors.toList());
        LOG.info("Loaded {} masks in memory", nmasks);

        LOG.info("Compute search results by library");

        List<ColorMIPSearchResult> allSearchResults = libraryMIPS.stream()
                .filter(libraryMIP -> libraryMIP.exists())
                .map(libraryMIP -> {
                    long libraryStartTime = System.currentTimeMillis();
                    LOG.info("Compare {} with {} masks", libraryMIP, nmasks);
                    List<CompletableFuture<ColorMIPSearchResult>> librarySearches = submitLibrarySearches(libraryMIP, masksMIPSWithImages);
                    return CompletableFuture.allOf(librarySearches.toArray(new CompletableFuture<?>[0]))
                            .thenApply(ignoredVoidResult -> librarySearches.stream()
                                    .map(searchComputation -> searchComputation.join())
                                    .filter(ColorMIPSearchResult::isMatch)
                                    .collect(Collectors.toList()))
                            .thenApply(matchingResults -> {
                                LOG.info("Found {} search results comparing {} masks with {} in {}s", matchingResults.size(), nmasks, libraryMIP, (System.currentTimeMillis() - libraryStartTime) / 1000);
                                return matchingResults;
                            })
                            .join()
                            ;
                })
                .flatMap(Collection::stream)
                .collect(Collectors.toList())
                ;
        LOG.info("Finished comparing {} masks with {} libraries in {}s", maskMIPS.size(), libraryMIPS.size(), (System.currentTimeMillis() - startTime) / 1000);
        return allSearchResults;
    }

    private List<CompletableFuture<ColorMIPSearchResult>> submitLibrarySearches(MIPInfo libraryMIP, List<Pair<MIPImage, MIPImage>> masksMIPSWithImages) {
        MIPImage libraryMIPImage = loadMIP(libraryMIP); // load image
        MIPImage libraryMIPGradient = loadGradientMIP(libraryMIP); // load gradient
        return masksMIPSWithImages.stream().parallel()
                .map(maskMIPWithGradient -> {
                    Supplier<ColorMIPSearchResult> searchResultSupplier = () -> {
                        ColorMIPSearchResult sr = runImageComparison(libraryMIPImage, maskMIPWithGradient.getLeft());
                        if (sr.isError()) {
                            LOG.warn("Errors encountered comparing {} with {}", maskMIPWithGradient.getLeft(), libraryMIPImage);
                        } else {
                            applyGradientAreaAdjustment(sr, libraryMIPImage, libraryMIPGradient, maskMIPWithGradient.getLeft(), maskMIPWithGradient.getRight());
                        }
                        return sr;
                    };
                    if (cdsExecutor == null) {
                        return CompletableFuture.supplyAsync(searchResultSupplier);
                    } else {
                        return CompletableFuture.supplyAsync(searchResultSupplier, cdsExecutor);
                    }
                })
                .collect(Collectors.toList())
                ;
    }
}
