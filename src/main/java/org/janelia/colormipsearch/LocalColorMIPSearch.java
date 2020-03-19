package org.janelia.colormipsearch;

import java.io.File;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
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
    private static final int DEFAULT_CDS_THREADS = 20;

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
                        int numberOfCdsThreads) {
        super(gradientMasksPath, outputPath, dataThreshold, maskThreshold, pixColorFluctuation, xyShift, negativeRadius, mirrorMask, pctPositivePixels);
        cdsExecutor = Executors.newFixedThreadPool(
                numberOfCdsThreads > 0 ? numberOfCdsThreads : DEFAULT_CDS_THREADS,
                new ThreadFactoryBuilder()
                        .setNameFormat("CDSRUNNER-%d")
                        .setDaemon(true)
                        .build());
    }

    @Override
    void compareEveryMaskWithEveryLibrary(List<MIPInfo> maskMIPS, List<MIPInfo> libraryMIPS) {
        long startTime = System.currentTimeMillis();
        int nmasks = maskMIPS.size();
        int nlibraries = libraryMIPS.size();

        LOG.info("Searching {} masks against {} libraries", nmasks, nlibraries);

        LOG.info("Load {} masks", nmasks);
        List<Pair<MIPImage, MIPImage>> masksMIPSWithImages = maskMIPS.stream().parallel()
                .filter(mip -> new File(mip.imageFilepath).exists())
                .map(mip -> ImmutablePair.of(loadMIP(mip), loadGradientMIP(mip)))
                .collect(Collectors.toList());
        LOG.info("Loaded {} masks in memory", nmasks);

        LOG.info("Compute and save search results by library");

        RetryPolicy<List<ColorMIPSearchResult>> retryPolicy = new RetryPolicy<List<ColorMIPSearchResult>>()
                .handle(IllegalStateException.class)
                .withDelay(Duration.ofMillis(500))
                .withMaxRetries(20);

        List<ColorMIPSearchResult> allSearchResults = libraryMIPS.stream()
                .filter(libraryMIP -> new File(libraryMIP.imageFilepath).exists())
                .map(libraryMIP -> {
                    long libraryStartTime = System.currentTimeMillis();
                    LOG.info("Compare {} with {} masks", libraryMIP, nmasks);
                    List<CompletableFuture<ColorMIPSearchResult>> librarySearches = submitLibrarySearches(libraryMIP, masksMIPSWithImages);
                    return CompletableFuture.allOf(librarySearches.toArray(new CompletableFuture<?>[0]))
                            .thenApplyAsync(ignoredVoidResult -> librarySearches.stream()
                                    .map(searchComputation -> searchComputation.join())
                                    .filter(ColorMIPSearchResult::isMatch)
                                    .collect(Collectors.toList()), cdsExecutor)
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

    private List<CompletableFuture<ColorMIPSearchResult>> submitLibrarySearches(MIPInfo libraryMIP, List<Pair<MIPImage, MIPImage>> masksMIPSWithImages) {
        MIPImage libraryMIPImage = loadMIP(libraryMIP); // load image
        MIPImage libraryMIPGradient = loadGradientMIP(libraryMIP); // load gradient
        return masksMIPSWithImages.stream()
                .map(maskMIPWithGradient -> CompletableFuture
                        .supplyAsync(() -> runImageComparison(libraryMIPImage, maskMIPWithGradient.getLeft()), cdsExecutor)
                        .thenApply(sr -> applyGradientAreaAdjustment(sr, libraryMIPImage, libraryMIPGradient, maskMIPWithGradient.getLeft(), maskMIPWithGradient.getRight()))
                        .whenComplete((sr, e) -> {
                            if (e != null || sr.isError) {
                                if (e != null) {
                                    LOG.error("Errors encountered comparing {} with {}", maskMIPWithGradient.getLeft(), libraryMIPImage, e);
                                } else {
                                    LOG.warn("Errors encountered comparing {} with {}", maskMIPWithGradient.getLeft(), libraryMIPImage);
                                }
                            }
                        }))
                .collect(Collectors.toList())
                ;
    }
}
