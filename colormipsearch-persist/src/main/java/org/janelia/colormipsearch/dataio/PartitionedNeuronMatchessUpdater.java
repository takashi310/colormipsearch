package org.janelia.colormipsearch.dataio;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.results.ItemsHandling;

public class PartitionedNeuronMatchessUpdater<M extends AbstractNeuronMetadata,
                                            T extends AbstractNeuronMetadata,
                                            R extends AbstractMatch<M, T>> implements NeuronMatchesUpdater<M, T, R> {
    private final NeuronMatchesUpdater<M, T, R> updater;
    private final int partitionSize;
    private final boolean parallel;

    public PartitionedNeuronMatchessUpdater(NeuronMatchesUpdater<M, T, R> updater, int partitionSize, boolean parallel) {
        this.updater = updater;
        this.partitionSize = partitionSize;
        this.parallel = parallel;
    }

    @Override
    public void writeUpdates(List<R> matches, List<Function<R, Pair<String, ?>>> fieldSelectors) {
        ItemsHandling.processPartitionStream(
                matches.stream(),
                partitionSize,
                partition -> updater.writeUpdates(partition, fieldSelectors),
                parallel
        );
    }
}
