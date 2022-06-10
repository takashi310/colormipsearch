package org.janelia.colormipsearch.api.pppsearch;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PPPUtils {
    private static final Logger LOG = LoggerFactory.getLogger(PPPUtils.class);

    public static EmPPPMatches readEmPPPMatchesFromJSONFile(File f, ObjectMapper objectMapper) {
        try {
            LOG.info("Read {}", f);
            return objectMapper.readValue(f, EmPPPMatches.class);
        } catch (IOException e) {
            LOG.error("Error reading PPP matches from {}", f, e);
            throw new IllegalArgumentException(e);
        }
    }

}
