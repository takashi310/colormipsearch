package org.janelia.colormipsearch.cmd;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import net.jodah.failsafe.Failsafe;
import org.janelia.colormipsearch.ColorMIPSearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Groups the results per mask mip ID and writes them to the specified directory - each mask mip ID is a file in the directory.
 */
class PerMaskColorMIPSearchResultsWriter extends AbstractColorMIPSearchResultsWriter {
    private static final Logger LOG = LoggerFactory.getLogger(PerMaskColorMIPSearchResultsWriter.class);

    @Override
    void writeSearchResults(Path outputPath, List<ColorMIPSearchResult> searchResults) {
        LOG.info("Group {} results by mask", searchResults.size());

        Map<String, List<ColorMIPSearchResult>> srsByMasks = searchResults.stream()
                .collect(Collectors.groupingBy(
                        ColorMIPSearchResult::getMaskId,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                l -> {
                                    l.sort(Comparator.comparing(ColorMIPSearchResult::getMatchingPixels).reversed());
                                    return l;
                                })));

        LOG.info("Write {} results by mask", srsByMasks.size());
        srsByMasks.forEach((maskId, srsForCurrentMask) -> Failsafe.with(retryPolicy).run(
                () -> writeSearchResultsToFile(
                        outputPath == null
                                ? null
                                : outputPath.resolve(maskId + ".json"),
                        srsForCurrentMask.stream().map(ColorMIPSearchResult::perMaskMetadata).collect(Collectors.toList()))));

        LOG.info("Finished writing {} results by mask", srsByMasks.size());
    }
}
