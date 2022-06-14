package org.janelia.colormipsearch.cmd;

import java.nio.file.Path;

import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

interface CDSDataInputGenerator {
    CDSDataInputGenerator prepare();

    void write(AbstractNeuronMetadata neuronMetadata);

    void done();
}
