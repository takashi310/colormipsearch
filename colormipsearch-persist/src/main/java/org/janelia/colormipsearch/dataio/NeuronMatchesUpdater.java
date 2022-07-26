package org.janelia.colormipsearch.dataio;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;

public interface NeuronMatchesUpdater<R extends AbstractMatch<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> {
    void writeUpdates(List<R> matches, List<Function<R, Pair<String, ?>>> fieldSelectors);
}
