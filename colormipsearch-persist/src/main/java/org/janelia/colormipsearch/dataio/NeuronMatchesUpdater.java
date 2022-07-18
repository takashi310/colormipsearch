package org.janelia.colormipsearch.dataio;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public interface NeuronMatchesUpdater<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>> {
    void writeUpdates(List<R> matches, List<Function<R, Pair<String, ?>>> fieldSelectors);
}
