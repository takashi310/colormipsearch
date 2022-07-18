package org.janelia.colormipsearch.dataio.fs;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.janelia.colormipsearch.dataio.CDMIPsReader;
import org.janelia.colormipsearch.dataio.InputParam;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONCDMIPsReader implements CDMIPsReader {
    private static final Logger LOG = LoggerFactory.getLogger(JSONCDMIPsReader.class);

    private final ObjectMapper mapper;

    public JSONCDMIPsReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public List<? extends AbstractNeuronMetadata> readMIPs(InputParam inputMipsParam) {
        try {
            LOG.info("Reading {} items from {} starting at {}",
                    (inputMipsParam.hasSize() ? String.valueOf(inputMipsParam.getSize()) : "all"), inputMipsParam.getValue(),
                    inputMipsParam.getOffset());
            List<? extends AbstractNeuronMetadata> content = mapper.readValue(
                    new File(inputMipsParam.getValue()),
                    new TypeReference<List<? extends AbstractNeuronMetadata>>() {});
            int from = (int) inputMipsParam.getOffset();
            int size = inputMipsParam.hasSize() ? inputMipsParam.getSize() : content.size();
            int to = Math.min(from + size, content.size());
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
