package org.janelia.colormipsearch.dataio;

import java.util.List;

import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public interface ResultMatchesUpdatesWriter<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>> {
    void writeUpdates(List<R> matches);
}
