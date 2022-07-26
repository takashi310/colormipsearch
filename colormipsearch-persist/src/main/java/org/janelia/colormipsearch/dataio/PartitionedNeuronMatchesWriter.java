package org.janelia.colormipsearch.dataio;

import java.util.List;

import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.results.ItemsHandling;

public class PartitionedNeuronMatchesWriter<R extends AbstractMatch<? extends AbstractNeuronEntity,
                                                                    ? extends AbstractNeuronEntity>>
        implements NeuronMatchesWriter<R> {
    private final NeuronMatchesWriter<R> writer;
    private final int partitionSize;
    private final boolean parallel;

    public PartitionedNeuronMatchesWriter(NeuronMatchesWriter<R> writer, int partitionSize, boolean parallel) {
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
