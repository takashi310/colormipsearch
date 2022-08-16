package org.janelia.colormipsearch.cmd.dataexport;

import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.CDMatchEntity;
import org.janelia.colormipsearch.model.PPPMatchEntity;

public class PPPMatchesExporter implements DataExporter {
    private final NeuronMatchesReader<PPPMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> neuronMatchesReader;

    public PPPMatchesExporter(NeuronMatchesReader<PPPMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> neuronMatchesReader) {
        this.neuronMatchesReader = neuronMatchesReader;
    }

    public void export(String source, long offset, int size) {
        // TODO
    }
}
