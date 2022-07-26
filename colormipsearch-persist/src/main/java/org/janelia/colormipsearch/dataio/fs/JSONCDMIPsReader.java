package org.janelia.colormipsearch.dataio.fs;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.janelia.colormipsearch.dataio.CDMIPsReader;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONCDMIPsReader implements CDMIPsReader {
    private static final Logger LOG = LoggerFactory.getLogger(JSONCDMIPsReader.class);

    private final ObjectMapper mapper;

    public JSONCDMIPsReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public List<? extends AbstractNeuronEntity> readMIPs(DataSourceParam inputMipsParam) {
        try {
            LOG.info("Reading {} items from {} starting at {}",
                    (inputMipsParam.hasSize() ? String.valueOf(inputMipsParam.getSize()) : "all"), inputMipsParam.getLocation(),
                    inputMipsParam.getOffset());
            List<? extends AbstractNeuronEntity> content = mapper.readValue(
                    new File(inputMipsParam.getLocation()),
                    new TypeReference<List<? extends AbstractNeuronEntity>>() {});
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
