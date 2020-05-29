package org.janelia.colormipsearch.cmd;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import net.jodah.failsafe.Failsafe;
import org.janelia.colormipsearch.tools.ColorMIPSearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Groups the results per library mip ID and writes them to the specified directory - each library mip ID is a file in the directory.
 */
class PerLibraryColorMIPSearchResultsWriter extends AbstractColorMIPSearchResultsWriter {
    private static final Logger LOG = LoggerFactory.getLogger(PerLibraryColorMIPSearchResultsWriter.class);

    @Override
    void writeSearchResults(Path outputPath, List<ColorMIPSearchResult> searchResults) {
        LOG.info("Group {} results by library", searchResults.size());

        Map<String, List<ColorMIPSearchResult>> srsByLibrary = searchResults.stream()
                .collect(Collectors.groupingBy(
                        ColorMIPSearchResult::getLibraryId,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                l -> {
                                    l.sort(Comparator.comparing(ColorMIPSearchResult::getMatchingPixels).reversed());
                                    return l;
                                })));

        LOG.info("Write {} results by library", srsByLibrary.size());
        srsByLibrary.entrySet().stream().parallel()
                .forEach(e -> {
                    String libraryId = e.getKey();
                    List<ColorMIPSearchResult> srsForCurrentLibrary = e.getValue();
                    Failsafe.with(retryPolicy).run(
                            () -> writeSearchResultsToFile(
                                    outputPath == null
                                            ? null
                                            : outputPath.resolve(libraryId + ".json"),
                                    srsForCurrentLibrary.stream().map(ColorMIPSearchResult::perLibraryMetadata).collect(Collectors.toList())));
                });

        LOG.info("Finished writing {} results by library", srsByLibrary.size());
    }
}
