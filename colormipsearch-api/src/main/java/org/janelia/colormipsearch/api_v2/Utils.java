package org.janelia.colormipsearch.api_v2;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);


    public static <T, R extends Results<List<T>>> void writeResultsToJSONFile(R results, File f, ObjectWriter objectWriter) {
        try {
            if (CollectionUtils.isNotEmpty(results.getResults())) {
                if (f == null) {
                    objectWriter.writeValue(System.out, results);
                } else {
                    LOG.info("Writing {}", f);
                    objectWriter.writeValue(f, results);
                }
            }
        } catch (IOException e) {
            LOG.error("Error writing CDS results to json file {}", f, e);
            throw new UncheckedIOException(e);
        }
    }

}
