package org.janelia.colormipsearch.tools;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColorMIPSearchResultUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ColorMIPSearchResultUtils.class);

    public static Results<List<ColorMIPSearchResultMetadata>> readCDSResultsFromJSONFile(File f, ObjectMapper mapper) {
        try {
            LOG.debug("Reading {}", f);
            return mapper.readValue(f, new TypeReference<Results<List<ColorMIPSearchResultMetadata>>>() {
            });
        } catch (IOException e) {
            LOG.error("Error reading CDS results from json file {}", f, e);
            throw new UncheckedIOException(e);
        }
    }

    public static void sortCDSResults(List<ColorMIPSearchResultMetadata> cdsResults) {
        Comparator<ColorMIPSearchResultMetadata> csrComp = (csr1, csr2) -> {
            if (csr1.getNormalizedScore() != null && csr2.getNormalizedScore() != null) {
                return Comparator.comparingDouble(ColorMIPSearchResultMetadata::getNormalizedScore)
                        .compare(csr1, csr2)
                        ;
            } else if (csr1.getNormalizedScore() == null && csr2.getNormalizedScore() == null) {
                return Comparator.comparingInt(ColorMIPSearchResultMetadata::getMatchingPixels)
                        .compare(csr1, csr2)
                        ;
            } else if (csr1.getNormalizedScore() == null) {
                // null gap scores should be at the beginning
                return -1;
            } else {
                return 1;
            }
        };
        cdsResults.sort(csrComp.reversed());
    }

    public static void writeCDSResultsToJSONFile(Results<List<ColorMIPSearchResultMetadata>> cdsResults, File f, ObjectMapper mapper) {
        try {
            if (CollectionUtils.isNotEmpty(cdsResults.results)) {
                if (f == null) {
                    mapper.writerWithDefaultPrettyPrinter().writeValue(System.out, cdsResults);
                } else {
                    LOG.info("Writing {}", f);
                    mapper.writerWithDefaultPrettyPrinter().writeValue(f, cdsResults);
                }
            }
        } catch (IOException e) {
            LOG.error("Error writing CDS results to json file {}", f, e);
            throw new UncheckedIOException(e);
        }
    }

}
