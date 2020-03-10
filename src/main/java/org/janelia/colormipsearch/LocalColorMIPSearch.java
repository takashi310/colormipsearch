package org.janelia.colormipsearch;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
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
        List<MIPWithPixels> masksMIPSWithImages = maskMIPS.stream().parallel()
                .filter(mip -> new File(mip.imageFilepath).exists())
                .map(this::loadMIP)
                .collect(Collectors.toList());
        LOG.info("Loaded {} masks in memory", nmasks);

        LOG.info("Compute and save search results by library");

        libraryMIPS.stream().parallel()
                .filter(libraryMIP -> new File(libraryMIP.imageFilepath).exists())
                .map(this::loadMIP)
                .forEach(libraryMIP -> {
                    long startLibraryComparison = System.currentTimeMillis();
                    LOG.info("Compare {} with {} masks", libraryMIP, nmasks);
                    masksMIPSWithImages
                            .forEach(maskMIP -> {
                                CompletableFuture
                                        .supplyAsync(() -> runImageComparison(libraryMIP, maskMIP), cdsExecutor)
                                        .thenApply(sr -> {
                                            if (sr.isMatch()) {
                                                // write the results directly - no sorting yet
                                                writeSearchResults(libraryMIP.id, Collections.singletonList(sr.perLibraryMetadata()));
                                                writeSearchResults(maskMIP.id, Collections.singletonList(sr.perMaskMetadata()));
                                            }
                                            return sr;
                                        })
                                        .whenComplete((sr, e) -> {
                                            if (e != null || sr.isError) {
                                                if (e != null) {
                                                    LOG.error("Errors encountered comparing {} with {}", maskMIP, libraryMIP, e);
                                                } else {
                                                    LOG.warn("Errors encountered comparing {} with {}", maskMIP, libraryMIP);
                                                }
                                            }
                                            LOG.debug("Completed comparing {} with {} mask after {}ms", libraryMIP, maskMIP, System.currentTimeMillis() - startLibraryComparison);
                                        })
                                        ;
                            })
                            ;
                })
                ;
        LOG.info("Submitted all color depth searches for {} masks against {} libraries in {}s", nlibraries, nmasks, (System.currentTimeMillis() - startTime) / 1000);
    }

}
