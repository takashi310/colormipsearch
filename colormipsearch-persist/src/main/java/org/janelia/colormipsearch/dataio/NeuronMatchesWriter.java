package org.janelia.colormipsearch.dataio;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.model.AbstractMatchEntity;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;

public interface NeuronMatchesWriter<R extends AbstractMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> {
    void write(List<R> matches);
    void writeUpdates(List<R> matches, List<Function<R, Pair<String, ?>>> fieldSelectors);
}
