package org.janelia.colormipsearch;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
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

        List<ColorMIPSearchResult> allSearchResults = libraryMIPS.stream()
                .filter(libraryMIP -> new File(libraryMIP.imageFilepath).exists())
                .map(libraryMIP -> ImmutablePair.of(loadMIP(libraryMIP), loadGradientMIP(libraryMIP)))
                .flatMap(libraryMIPWithGradient -> {
                    long startLibraryComparison = System.currentTimeMillis();
                    LOG.info("Compare {} with {} masks", libraryMIPWithGradient.getLeft(), nmasks);
                    List<CompletableFuture<ColorMIPSearchResult>> librarySearches = masksMIPSWithImages.stream()
                            .map(maskMIPWithGradient -> CompletableFuture
                                    .supplyAsync(() -> runImageComparison(libraryMIPWithGradient.getLeft(), maskMIPWithGradient.getLeft()), cdsExecutor)
                                    .thenApplyAsync(sr -> applyGradientAreaAdjustment(sr, libraryMIPWithGradient.getLeft(), libraryMIPWithGradient.getRight(), maskMIPWithGradient.getLeft(), maskMIPWithGradient.getRight()), cdsExecutor)
                                    .whenComplete((sr, e) -> {
                                        if (e != null || sr.isError) {
                                            if (e != null) {
                                                LOG.error("Errors encountered comparing {} with {}", maskMIPWithGradient.getLeft(), libraryMIPWithGradient.getLeft(), e);
                                            } else {
                                                LOG.warn("Errors encountered comparing {} with {}", maskMIPWithGradient.getLeft(), libraryMIPWithGradient.getLeft());
                                            }
                                        }
                                        LOG.debug("Completed comparing {} with {} after {}ms", maskMIPWithGradient.getLeft(), libraryMIPWithGradient.getLeft(), System.currentTimeMillis() - startLibraryComparison);
                                    }))
                            .collect(Collectors.toList())
                            ;
                    List<ColorMIPSearchResult> librarySearchResults = CompletableFuture.allOf(librarySearches.toArray(new CompletableFuture<?>[0]))
                            .thenApply(vr -> librarySearches.stream().map(CompletableFuture::join).collect(Collectors.toList()))
                            .thenApply(srs -> srs.stream().filter(ColorMIPSearchResult::isMatch).collect(Collectors.toList()))
                            .thenApplyAsync(matchingResults -> {
                                LOG.info("Found {} search results after comparing {} masks with {} in {}s", matchingResults.size(), nmasks, libraryMIPWithGradient.getLeft(), (System.currentTimeMillis() - startTime) / 1000);
                                writeSearchResults(libraryMIPWithGradient.getLeft().mipInfo.id, matchingResults.stream().map(ColorMIPSearchResult::perLibraryMetadata).collect(Collectors.toList()));
                                return matchingResults;
                            }, cdsExecutor)
                            .join()
                            ;
                    return librarySearchResults.stream();
                })
                .collect(Collectors.toList())
                ;

        LOG.info("Finished comparing {} masks with {} libraries in {}s", maskMIPS.size(), libraryMIPS.size(), (System.currentTimeMillis() - startTime) / 1000);
        Map<String, List<ColorMIPSearchResult>> srsByMasks = allSearchResults.stream()
                .collect(Collectors.groupingBy(ColorMIPSearchResult::getMaskId, Collectors.toList()));

        LOG.info("Write {} results by mask", srsByMasks.size());
        srsByMasks.forEach((maskId, srsForCurrentMask) -> {
            writeSearchResults(maskId, srsForCurrentMask.stream().map(ColorMIPSearchResult::perMaskMetadata).collect(Collectors.toList()));
        });

        LOG.info("Finished writing results after comparing {} masks with {} libraries in {}s", maskMIPS.size(), libraryMIPS.size(), (System.currentTimeMillis() - startTime) / 1000);
    }

}
