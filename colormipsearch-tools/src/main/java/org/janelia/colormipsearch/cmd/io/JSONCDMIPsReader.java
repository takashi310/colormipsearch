package org.janelia.colormipsearch.cmd.io;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONCDMIPsReader implements CDMIPsReader {
    private static final Logger LOG = LoggerFactory.getLogger(JSONCDMIPsReader.class);

    private final ObjectMapper mapper;

    public JSONCDMIPsReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public List<? extends AbstractNeuronMetadata> readMIPs(String library, long offset, int length) {
        try {
            LOG.info("Reading {} items from {} starting at {}",
                    (length > 0 ? String.valueOf(length) : "all"), library, offset);
            List<? extends AbstractNeuronMetadata> content = mapper.readValue(
                    new File(library),
                    new TypeReference<List<? extends AbstractNeuronMetadata>>() {});
            int from = offset > 0 ? (int) offset : 0;
            int to = length > 0 ? Math.min(from + length, content.size()) : content.size();
            if (from > 0 || to < content.size()) {
                return content.subList(from, to);
            } else {
                return content;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
