package org.janelia.colormipsearch.cmd.dataexport;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.fileutils.JSONFileGroupedItemsWriter;
import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.datarequests.ScoresFilter;
import org.janelia.colormipsearch.datarequests.SortCriteria;
import org.janelia.colormipsearch.datarequests.SortDirection;
import org.janelia.colormipsearch.dto.AbstractNeuronMetadata;
import org.janelia.colormipsearch.dto.CDMatchedTarget;
import org.janelia.colormipsearch.dto.ResultMatches;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.CDMatchEntity;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.janelia.colormipsearch.results.MatchResultsGrouping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerMaskCDMatchesExporter implements DataExporter {
    private static final Logger LOG = LoggerFactory.getLogger(PerMaskCDMatchesExporter.class);

    private final DataSourceParam dataSourceParam;
    private final ScoresFilter scoresFilter;
    private final Path outputDir;
    private final NeuronMatchesReader<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> neuronMatchesReader;
    private final JSONFileGroupedItemsWriter resultMatchesWriter;
    private final int processingPartitionSize;

    public PerMaskCDMatchesExporter(DataSourceParam dataSourceParam,
                                    ScoresFilter scoresFilter,
                                    Path outputDir,
                                    NeuronMatchesReader<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> neuronMatchesReader,
                                    JSONFileGroupedItemsWriter resultMatchesWriter,
                                    int processingPartitionSize) {
        this.dataSourceParam = dataSourceParam;
        this.scoresFilter = scoresFilter;
        this.outputDir = outputDir;
        this.neuronMatchesReader = neuronMatchesReader;
        this.resultMatchesWriter = resultMatchesWriter;
        this.processingPartitionSize = processingPartitionSize;
    }

    @Override
    public DataSourceParam getDataSource() {
        return dataSourceParam;
    }

    @Override
    public void runExport() {
        List<String> masks = neuronMatchesReader.listMatchesLocations(Collections.singletonList(dataSourceParam));
        ItemsHandling.partitionCollection(masks, processingPartitionSize).entrySet().stream().parallel()
                .forEach(indexedPartition -> {
                    indexedPartition.getValue().forEach(maskId -> {
                        LOG.info("Read color depth matches for {}", maskId);
                        List<CDMatchEntity<?, ?>> matchesForMask = neuronMatchesReader.readMatchesForMasks(
                                null,
                                Collections.singletonList(maskId),
                                scoresFilter,
                                Collections.singletonList(
                                        new SortCriteria("normalizedScore", SortDirection.DESC)
                                ));
                        LOG.info("Write color depth matches for {}", maskId);
                        writeResults(matchesForMask);
                    });
                });
    }

    private <M extends AbstractNeuronMetadata> void
    writeResults(List<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> matches) {
        // group results by mask MIP ID
        List<Function<M, ?>> grouping = Collections.singletonList(
                AbstractNeuronMetadata::getMipId
        );
        // order descending by normalized score
        Comparator<CDMatchedTarget<? extends AbstractNeuronMetadata>> ordering = Comparator.comparingDouble(m -> -m.getNormalizedScore());
        List<ResultMatches<M, CDMatchedTarget<?>>> matchesByMask = MatchResultsGrouping.groupByMask(
                matches,
                grouping,
                ordering);
        // write results by mask MIP ID
        resultMatchesWriter.writeGroupedItemsList(matchesByMask, AbstractNeuronMetadata::getMipId, outputDir);
    }
}
