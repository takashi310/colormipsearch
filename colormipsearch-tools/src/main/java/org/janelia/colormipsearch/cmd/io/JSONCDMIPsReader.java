package org.janelia.colormipsearch.cmd.io;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public class JSONCDMIPsReader implements CDMIPsReader {
    private final ObjectMapper mapper;

    public JSONCDMIPsReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public List<? extends AbstractNeuronMetadata> readMIPs(String library, long offset, int length) {
        return new ArrayList<>() ; // !!!!!!!!!! FIXME
    }
}
