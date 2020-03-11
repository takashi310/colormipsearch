package org.janelia.colormipsearch;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

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

        List<CompletableFuture<ColorMIPSearchResult>> cdSearches = libraryMIPS.stream().parallel()
                .filter(libraryMIP -> new File(libraryMIP.imageFilepath).exists())
                .map(libraryMIP -> ImmutablePair.of(loadMIP(libraryMIP), loadGradientMIP(libraryMIP)))
                .flatMap(libraryMIPWithGradient -> {
                    long startLibraryComparison = System.currentTimeMillis();
                    LOG.info("Compare {} with {} masks", libraryMIPWithGradient.getLeft(), nmasks);
                    return masksMIPSWithImages.stream()
                            .map(maskMIPWithGradient -> CompletableFuture
                                    .supplyAsync(() -> runImageComparison(libraryMIPWithGradient.getLeft(), maskMIPWithGradient.getLeft()), cdsExecutor)
                                    .thenApplyAsync(sr -> applyGradientAreaAdjustment(sr, libraryMIPWithGradient.getLeft(), libraryMIPWithGradient.getRight(), maskMIPWithGradient.getLeft(), maskMIPWithGradient.getRight()), cdsExecutor)
                                    .thenApply(sr -> {
                                        if (sr.isMatch()) {
                                            // write the results directly - no sorting yet
                                            writeSearchResults(libraryMIPWithGradient.getLeft().mipInfo.id, Collections.singletonList(sr.perLibraryMetadata()));
                                            writeSearchResults(maskMIPWithGradient.getLeft().mipInfo.id, Collections.singletonList(sr.perMaskMetadata()));
                                        }
                                        return sr;
                                    })
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
                            ;
                })
                .collect(Collectors.toList())
                ;

        LOG.info("Submitted all color depth searches for comparing {} masks with {} libraries in {}s", nmasks, nlibraries, (System.currentTimeMillis() - startTime) / 1000);

        CompletableFuture.allOf(cdSearches.toArray(new CompletableFuture<?>[0]))
                .thenApplyAsync(vr -> cdSearches.stream().map(CompletableFuture::join).collect(Collectors.toList()), cdsExecutor)
                .thenApplyAsync(searchResults -> {
                    LOG.info("Found {} search results after comparing {} masks with {} libraries in {}s", searchResults.size(), nmasks, nlibraries, (System.currentTimeMillis() - startTime) / 1000);
                    return searchResults.size();
                }, cdsExecutor)
                .join()
                ;

        LOG.info("Finished searching {} masks against {} libraries in {}s", maskMIPS.size(), libraryMIPS.size(), (System.currentTimeMillis() - startTime) / 1000);
    }

}
