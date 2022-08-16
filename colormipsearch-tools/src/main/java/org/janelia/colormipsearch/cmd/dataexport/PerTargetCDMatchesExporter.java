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

public class PerTargetCDMatchesExporter implements DataExporter {
    private static final Logger LOG = LoggerFactory.getLogger(PerMaskCDMatchesExporter.class);

    private final NeuronMatchesReader<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> neuronMatchesReader;
    private final ScoresFilter scoresFilter;
    private final int processingPartitionSize;

    public PerTargetCDMatchesExporter(NeuronMatchesReader<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> neuronMatchesReader,
                                      ScoresFilter scoresFilter,
                                      int processingPartitionSize) {
        this.neuronMatchesReader = neuronMatchesReader;
        this.scoresFilter = scoresFilter;
        this.processingPartitionSize = processingPartitionSize;
    }

    /**
     * Export matches for targets from the specified library.
     *
     * @param targetLibrary target library
     * @param offset
     * @param size
     */
    public void export(String targetLibrary, long offset, int size) {
        List<String> masks = neuronMatchesReader.listMatchesLocations(
                Collections.singletonList(new DataSourceParam(targetLibrary, offset, size)));
        ItemsHandling.partitionCollection(masks, processingPartitionSize).stream().parallel()
                .forEach(partititionItems -> {
                    partititionItems.forEach(targetId -> {
                        LOG.info("Read color depth matches for {}", targetId);
                        List<CDMatchEntity<?, ?>> matchesForTarget = neuronMatchesReader.readMatchesForTargets(
                                null,
                                Collections.singletonList(targetId),
                                scoresFilter,
                                Collections.singletonList(
                                        new SortCriteria("normalizedScore", SortDirection.DESC)
                                ));
                        LOG.info("Write color depth matches for {}", targetId);
                        // TODO

//                        perMaskNeuronMatchesWriter.write(matchesForMask);
                    });
                });

        // TODO
    }
}
