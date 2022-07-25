package org.janelia.colormipsearch.dataio;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.results.ItemsHandling;

public class PartitionedNeuronMatchesUpdater<R extends AbstractMatch<? extends AbstractNeuronMetadata,
                                                                     ? extends AbstractNeuronMetadata>>
        implements NeuronMatchesUpdater<R> {
    private final NeuronMatchesUpdater<R> updater;
    private final int partitionSize;
    private final boolean parallel;

    public PartitionedNeuronMatchesUpdater(NeuronMatchesUpdater<R> updater, int partitionSize, boolean parallel) {
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
