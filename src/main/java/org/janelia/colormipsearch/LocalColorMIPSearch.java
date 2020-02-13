package org.janelia.colormipsearch;

import java.io.File;
import java.util.List;
import java.util.Map;
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

    LocalColorMIPSearch(String outputPath,
                        Integer dataThreshold, Double pixColorFluctuation, Integer xyShift,
                        boolean mirrorMask, Double pctPositivePixels,
                        int numberOfCdsThreads) {
        super(outputPath, dataThreshold, pixColorFluctuation, xyShift, mirrorMask, pctPositivePixels);
        cdsExecutor = Executors.newFixedThreadPool(
                numberOfCdsThreads > 0 ? numberOfCdsThreads : DEFAULT_CDS_THREADS,
                new ThreadFactoryBuilder()
                        .setNameFormat("CDSRUNNER-%d")
                        .setDaemon(true)
                        .build());
    }

    void  compareEveryMaskWithEveryLibrary(List<MIPInfo> maskMIPS, List<MIPInfo> libraryMIPS, Integer maskThreshold) {
        long startTime = System.currentTimeMillis();
        int nmasks = maskMIPS.size();
        int nlibraries = libraryMIPS.size();

        LOG.info("Searching {} masks against {} libraries", nmasks, nlibraries);

        LOG.info("Load {} masks", nmasks);
        List<MIPWithPixels> masksMIPSWithImages = maskMIPS.stream().parallel()
                .filter(mip -> new File(mip.filepath).exists())
                .map(this::loadMIP)
                .collect(Collectors.toList());
        LOG.info("Loaded {} masks in memory", nmasks);

        LOG.info("Compute and save search results by library");

        List<Pair<String, List<ColorMIPSearchResult>>> srByLibraries = libraryMIPS.stream().parallel()
                .map(this::loadMIP)
                .map(libraryMIP -> {
                    long startLibraryComparison = System.currentTimeMillis();
                    LOG.info("Compare {} with {} masks", libraryMIP, nmasks);
                    List<CompletableFuture<ColorMIPSearchResult>> srForLibFutures = masksMIPSWithImages.stream()
                            .filter(mip -> new File(mip.filepath).exists())
                            .map(maskMIP -> CompletableFuture.supplyAsync(() -> runImageComparison(libraryMIP, maskMIP, maskThreshold), cdsExecutor))
                            .collect(Collectors.toList())
                            ;
                    try {
                        return CompletableFuture.allOf(srForLibFutures.toArray(new CompletableFuture<?>[0]))
                                .thenApplyAsync(vr -> srForLibFutures.stream().map(CompletableFuture::join), cdsExecutor)
                                .thenApplyAsync(srsStreamForCurrentLibrary -> srsStreamForCurrentLibrary
                                        .filter(ColorMIPSearchResult::isMatch)
                                        .sorted(getColorMIPSearchComparator())
                                        .collect(Collectors.toList()), cdsExecutor)
                                .thenApplyAsync(srsForCurrentLibrary -> {
                                    LOG.info("Write {} search results for all masks against {} ", srsForCurrentLibrary.size(), libraryMIP);
                                    writeSearchResults(libraryMIP.id, srsForCurrentLibrary.stream()
                                            .map(ColorMIPSearchResult::perLibraryMetadata)
                                            .collect(Collectors.toList())
                                    );
                                    return ImmutablePair.of(libraryMIP.id, srsForCurrentLibrary);
                                }, cdsExecutor)
                                .join()
                                ;
                    } finally {
                        LOG.info("Completed comparing {} with {} masks in {}ms", libraryMIP, nmasks, System.currentTimeMillis() - startLibraryComparison);
                    }
                })
                .collect(Collectors.toList())
                ;
        LOG.info("Saved {} search results by library for {} libraries against {} masks in {}s", srByLibraries.size(), nlibraries, nmasks, (System.currentTimeMillis() - startTime) / 1000);

        Map<String, List<ColorMIPSearchResultMetadata>> srByMasks = srByLibraries.stream().parallel()
                .flatMap(srByLibrary -> srByLibrary.getRight().stream().parallel())
                .collect(Collectors.groupingBy(ColorMIPSearchResult::getMaskId, Collectors.collectingAndThen(Collectors.toList(),
                        srByMask -> srByMask.stream()
                                .sorted(getColorMIPSearchComparator())
                                .map(ColorMIPSearchResult::perMaskMetadata)
                                .collect(Collectors.toList())
                )))
                ;
        srByMasks.entrySet()
                .forEach(srByMask -> writeSearchResults(srByMask.getKey(), srByMask.getValue()));
        LOG.info("Saved {} search results by mask for {} masks against {} libraries in {}s", srByMasks.size(), nmasks, nlibraries, (System.currentTimeMillis() - startTime) / 1000);

        LOG.info("Finished searching {} masks against {} libraries in {}s", maskMIPS.size(), libraryMIPS.size(), (System.currentTimeMillis() - startTime) / 1000);
    }

}
