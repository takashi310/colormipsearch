package org.janelia.colormipsearch.cmd.dataexport;

import java.util.Collections;
import java.util.List;

import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.datarequests.ScoresFilter;
import org.janelia.colormipsearch.datarequests.SortCriteria;
import org.janelia.colormipsearch.datarequests.SortDirection;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.CDMatchEntity;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerMaskCDMatchesExporter implements DataExporter {
    private static final Logger LOG = LoggerFactory.getLogger(PerMaskCDMatchesExporter.class);

    private final NeuronMatchesReader<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> neuronMatchesReader;
    private final ScoresFilter scoresFilter;
    private final int processingPartitionSize;

    public PerMaskCDMatchesExporter(NeuronMatchesReader<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> neuronMatchesReader,
                                    ScoresFilter scoresFilter,
                                    int processingPartitionSize) {
        this.neuronMatchesReader = neuronMatchesReader;
        this.scoresFilter = scoresFilter;
        this.processingPartitionSize = processingPartitionSize;
    }

    public void export(String source, long offset, int size) {
        List<String> masks = neuronMatchesReader.listMatchesLocations(
                Collections.singletonList(new DataSourceParam(source, offset, size)));
        ItemsHandling.partitionCollection(masks, processingPartitionSize).stream().parallel()
                .forEach(partititionItems -> {
                    partititionItems.forEach(maskId -> {
                        LOG.info("Read color depth matches for {}", maskId);
                        List<CDMatchEntity<?, ?>> matchesForMask = neuronMatchesReader.readMatchesForMasks(
                                null,
                                Collections.singletonList(maskId),
                                scoresFilter,
                                Collections.singletonList(
                                        new SortCriteria("normalizedScore", SortDirection.DESC)
                                ));
                        LOG.info("Write color depth matches for {}", maskId);
                    // TODO

//                        perMaskNeuronMatchesWriter.write(matchesForMask);
                    });
                });

        // TODO
    }
}
