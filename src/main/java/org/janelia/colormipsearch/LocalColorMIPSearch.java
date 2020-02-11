package org.janelia.colormipsearch;

import java.io.File;
import java.util.List;
import java.util.Map;
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

    LocalColorMIPSearch(String outputPath,
                        Integer dataThreshold, Double pixColorFluctuation, Integer xyShift,
                        boolean mirrorMask, Double pctPositivePixels) {
        super(outputPath, dataThreshold, pixColorFluctuation, xyShift, mirrorMask, pctPositivePixels);
    }

    void  compareEveryMaskWithEveryLibrary(List<MinimalColorDepthMIP> maskMIPS, List<MinimalColorDepthMIP> libraryMIPS, Integer maskThreshold) {
        long startTime = System.currentTimeMillis();
        int nmasks = maskMIPS.size();
        int nlibraries = libraryMIPS.size();

        LOG.info("Searching {} masks against {} libraries", nmasks, nlibraries);

        LOG.info("Load {} masks", nmasks);
        List<MIPWithImage> masksMIPSWithImages = maskMIPS.stream().parallel()
                .filter(mip -> new File(mip.filepath).exists())
                .map(this::loadMIP)
                .collect(Collectors.toList());
        LOG.info("Loaded {} masks in memory", nmasks);

        LOG.info("Compute and save search results by library");

        List<Pair<String, List<ColorMIPSearchResult>>> srByLibraries = libraryMIPS.stream()
                .map(this::loadMIP).parallel()
                .map(libraryMIP -> {
                    long startLibraryComparison = System.currentTimeMillis();
                    LOG.info("Compare {} with {} masks", libraryMIP, nmasks);
                    List<ColorMIPSearchResult> srForCurrentLibrary = masksMIPSWithImages.stream().parallel()
                            .filter(mip -> new File(mip.filepath).exists())
                            .map(maskMIP -> runImageComparison(libraryMIP, maskMIP, maskThreshold))
                            .filter(ColorMIPSearchResult::isMatch)
                            .sorted(getColorMIPSearchComparator())
                            .collect(Collectors.toList());
                    LOG.info("Done comparing {} with {} masks in {}ms", libraryMIP, nmasks, System.currentTimeMillis() - startLibraryComparison);

                    LOG.info("Write {} search results for {}", srForCurrentLibrary.size(), libraryMIP);
                    writeSearchResults(libraryMIP.id, srForCurrentLibrary);
                    LOG.info("Written {} search results for {} (processing time {}ms)", srForCurrentLibrary.size(), libraryMIP, System.currentTimeMillis() - startLibraryComparison);
                    return ImmutablePair.of(libraryMIP.id, srForCurrentLibrary);
                })
                .collect(Collectors.toList())
                ;
        LOG.info("Saved {} search results by library for {} libraries against {} masks in {}s", srByLibraries.size(), nlibraries, nmasks, (System.currentTimeMillis() - startTime) / 1000);

        Map<String, List<ColorMIPSearchResult>> srByMasks = srByLibraries.stream().parallel()
                .flatMap(srByLibrary -> srByLibrary.getRight().stream().parallel())
                .collect(Collectors.groupingBy(ColorMIPSearchResult::getPatternId, Collectors.collectingAndThen(Collectors.toList(), srByMask -> {
                    srByMask.sort(getColorMIPSearchComparator());
                    return srByMask;
                })))
                ;
        srByMasks.entrySet()
                .forEach(srByMask -> writeSearchResults(srByMask.getKey(), srByMask.getValue()));
        LOG.info("Saved {} search results by mask for {} masks against {} libraries in {}s", srByMasks.size(), nmasks, nlibraries, (System.currentTimeMillis() - startTime) / 1000);

        LOG.info("Finished searching {} masks against {} libraries in {}s", maskMIPS.size(), libraryMIPS.size(), (System.currentTimeMillis() - startTime) / 1000);
    }

}
