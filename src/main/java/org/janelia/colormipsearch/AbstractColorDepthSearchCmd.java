package org.janelia.colormipsearch;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AbstractColorDepthSearchCmd {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractColorDepthSearchCmd.class);

    void saveCDSParameters(ColorMIPSearch colorMIPSearch, Path outputDir, String fname) {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        if (outputDir != null) {
            File outputFile = outputDir.resolve(fname).toFile();
            try {
                mapper.writerWithDefaultPrettyPrinter().
                        writeValue(outputFile, colorMIPSearch.getCDSParameters());
            } catch (IOException e) {
                LOG.error("Error persisting color depth search parameters to {}", outputFile, e);
                throw new IllegalStateException(e);
            }
        }
    }

}
