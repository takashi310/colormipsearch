package org.janelia.colormipsearch.io;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import com.fasterxml.jackson.databind.ObjectWriter;

import org.janelia.colormipsearch.api_v2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonOutputHelper {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static <V> void writeToJSONFile(V v, File f, ObjectWriter objectWriter) {
        try {
            if (v != null) {
                if (f == null) {
                    objectWriter.writeValue(System.out, v);
                } else {
                    LOG.info("Writing {}", f);
                    objectWriter.writeValue(f, v);
                }
            }
        } catch (IOException e) {
            LOG.error("Error writing to json file {}", f, e);
            throw new UncheckedIOException(e);
        }
    }
}
