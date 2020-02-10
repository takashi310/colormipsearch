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
                        .peek(sr -> {
                            if (sr.isError()) {
                                LOG.warn("Error encountered in {}", sr);
                            }
                        })
                        .filter(ColorMIPSearchResult::isMatch)
                )
                ;

        // write results by library
        LOG.info("Group results by library");
        Map<String, List<ColorMIPSearchResult>> srByLibrary = srStream
                .collect(Collectors.groupingBy(ColorMIPSearchResult::getLibraryId, Collectors.toList()));

        LOG.info("Write results by library");
        writeAllSearchResults(srByLibrary);

        // write results by mask
        LOG.info("Group results by mask");
        Map<String, List<ColorMIPSearchResult>> srByMask = srByLibrary.entrySet().parallelStream()
                .flatMap(srsByLibrary -> srsByLibrary.getValue().stream())
                .collect(Collectors.groupingBy(ColorMIPSearchResult::getPatternId, Collectors.toList()))
                ;

        LOG.info("Write results by mask");
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
