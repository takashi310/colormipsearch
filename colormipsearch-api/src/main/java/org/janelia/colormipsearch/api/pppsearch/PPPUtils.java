package org.janelia.colormipsearch.api.pppsearch;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.commons.collections4.CollectionUtils;
import org.janelia.colormipsearch.api.Results;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PPPUtils {
    private static final Logger LOG = LoggerFactory.getLogger(PPPUtils.class);

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
