package org.janelia.colormipsearch.cmd.io;

import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public interface CDSDataInputGenerator {
    CDSDataInputGenerator prepare();

    void write(AbstractNeuronMetadata neuronMetadata);

    void done();
}
