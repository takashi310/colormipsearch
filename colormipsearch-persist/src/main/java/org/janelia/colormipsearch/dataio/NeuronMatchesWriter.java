package org.janelia.colormipsearch.dataio;

import java.util.List;

import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;

public interface NeuronMatchesWriter<R extends AbstractMatch<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> {
    void write(List<R> matches);
}
