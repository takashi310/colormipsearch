package org.janelia.colormipsearch.api.pppsearch;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.commons.collections4.CollectionUtils;
import org.janelia.colormipsearch.api.Results;
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
