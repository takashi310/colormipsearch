package org.janelia.colormipsearch.dataio;

import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public interface CDSDataInputGenerator {
    void write(AbstractNeuronMetadata neuronMetadata);

    void done();
}
