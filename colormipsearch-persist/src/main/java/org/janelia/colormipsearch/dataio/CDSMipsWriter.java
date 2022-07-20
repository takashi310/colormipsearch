package org.janelia.colormipsearch.dataio;

import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public interface CDSMipsWriter {
    void write(AbstractNeuronMetadata neuronMetadata);
    void done();
}
