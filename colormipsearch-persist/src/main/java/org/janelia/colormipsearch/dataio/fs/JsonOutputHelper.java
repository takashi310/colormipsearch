package org.janelia.colormipsearch.dataio.fs;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.janelia.colormipsearch.dataio.fileutils.FSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonOutputHelper {
    private static final Logger LOG = LoggerFactory.getLogger(JsonOutputHelper.class);

    public static void writeJSONNode(JsonNode v, Path p, ObjectWriter objectWriter) {
        try {
            if (v != null) {
                if (p == null) {
                    objectWriter.writeValue(System.out, v);
                } else {
                    LOG.info("Writing {}", p);
                    FSUtils.createDirs(p.getParent());
                    objectWriter.writeValue(p.toFile(), v);
                }
            }
        } catch (IOException e) {
            LOG.error("Error writing to json file {}", p, e);
            throw new UncheckedIOException(e);
        }
    }

    public static <V> void writeToJSONFile(V v, Path p, ObjectWriter objectWriter) {
        try {
            if (v != null) {
                if (p == null) {
                    LOG.info("Writing JSON to STDOUT");
                    objectWriter.writeValue(System.out, v);
                } else {
                    LOG.info("Writing {}", p);
                    objectWriter.writeValue(p.toFile(), v);
                }
            }
        } catch (IOException e) {
            LOG.error("Error writing to json file {}", p, e);
            throw new UncheckedIOException(e);
        }
    }
}
