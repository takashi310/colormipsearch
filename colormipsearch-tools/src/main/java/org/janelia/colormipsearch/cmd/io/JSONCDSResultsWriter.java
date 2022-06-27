package org.janelia.colormipsearch.cmd.io;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDSMatch;

public class JSONCDSResultsWriter<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> implements ResultMatchesWriter<M, T, CDSMatch<M, T>> {
    private final ObjectMapper mapper;

    public JSONCDSResultsWriter(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public void write(List<CDSMatch<M, T>> cdsMatches) {

    }
}
