package org.janelia.colormipsearch.dataio;

import java.util.List;

import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.results.ItemsHandling;

public class PartitionedNeuronMatchesWriter<M extends AbstractNeuronMetadata,
                                            T extends AbstractNeuronMetadata,
                                            R extends AbstractMatch<M, T>> implements NeuronMatchesWriter<M, T, R> {
    private final NeuronMatchesWriter<M, T, R> writer;
    private final int partitionSize;
    private final boolean parallel;

    public PartitionedNeuronMatchesWriter(NeuronMatchesWriter<M, T, R> writer, int partitionSize, boolean parallel) {
        this.writer = writer;
        this.partitionSize = partitionSize;
        this.parallel = parallel;
    }

    @Override
    public void write(List<R> matches) {
        ItemsHandling.processPartitionStream(
                matches.stream(),
                partitionSize,
                writer::write,
                parallel
        );
    }
}
