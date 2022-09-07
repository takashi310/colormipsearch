package org.janelia.colormipsearch.dataio;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.model.AbstractMatchEntity;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.results.ItemsHandling;

public class PartitionedNeuronMatchesWriter<R extends AbstractMatchEntity<? extends AbstractNeuronEntity,
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
        Stream<Map.Entry<Integer, List<R>>> partitionsStream;
        if (parallel) {
            partitionsStream = ItemsHandling.partitionCollection(matches, partitionSize).entrySet().stream().parallel();
        } else {
            partitionsStream = ItemsHandling.partitionCollection(matches, partitionSize).entrySet().stream();
        }
        partitionsStream.forEach(e -> writer.write(e.getValue()));
    }

    @Override
    public void writeUpdates(List<R> matches, List<Function<R, Pair<String, ?>>> fieldSelectors) {
        Stream<Map.Entry<Integer, List<R>>> partitionsStream;
        if (parallel) {
            partitionsStream = ItemsHandling.partitionCollection(matches, partitionSize).entrySet().stream().parallel();
        } else {
            partitionsStream = ItemsHandling.partitionCollection(matches, partitionSize).entrySet().stream();
        }
        partitionsStream.forEach(e -> writer.writeUpdates(e.getValue(), fieldSelectors));
    }

}
