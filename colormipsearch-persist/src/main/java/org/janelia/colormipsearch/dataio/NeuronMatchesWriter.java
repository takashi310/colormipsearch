package org.janelia.colormipsearch.dataio;

import java.util.List;

import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public interface NeuronMatchesWriter<R extends AbstractMatch<? extends AbstractNeuronMetadata, ? extends AbstractNeuronMetadata>> {
    void write(List<R> matches);
}
