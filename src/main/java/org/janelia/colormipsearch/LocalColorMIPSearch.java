package org.janelia.colormipsearch;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        LOG.info("Searching {} masks against {} libraries", maskMIPS.size(), libraryMIPS.size());

        Stream<ColorMIPSearchResult> srStream = libraryMIPS.stream()
                .filter(mip -> new File(mip.filepath).exists())
                .parallel()
                .map(this::loadMIP)
                .flatMap(libraryMIP -> maskMIPS.stream()
                        .filter(mip -> new File(mip.filepath).exists())
                        .parallel()
                        .map(this::loadMIP)
                        .map(maskMIP -> runImageComparison(libraryMIP, maskMIP, maskThreshold))
                        .filter(sr -> sr.isMatch() || sr.isError())
                )
                ;

        // write results by library
        Map<String, List<ColorMIPSearchResult>> srByLibrary = srStream
                .filter(ColorMIPSearchResult::isMatch)
                .collect(
                        Collectors.groupingBy(ColorMIPSearchResult::getLibraryId, Collectors.toList()));

        writeAllSearchResults(srByLibrary);

        // write results by mask
        Map<String, List<ColorMIPSearchResult>> srByMask = srByLibrary.entrySet().parallelStream()
                .flatMap(srsByLibrary -> srsByLibrary.getValue().stream())
                .collect(Collectors.groupingBy(ColorMIPSearchResult::getPatternId, Collectors.toList()))
                ;
        writeAllSearchResults(srByMask);
    }

    private void writeAllSearchResults(Map<String, List<ColorMIPSearchResult>> searchResults) {
        searchResults.entrySet().parallelStream()
                .forEach(groupedSrs -> {
                    writeSearchResults(groupedSrs.getKey(), groupedSrs.getValue().stream().sorted(getColorMIPSearchComparator()).collect(Collectors.toList()));
                })
        ;
    }

}
