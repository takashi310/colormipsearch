package org.janelia.colormipsearch.cmd.dataexport;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import org.janelia.colormipsearch.cmd.jacsdata.CachedJacsDataHelper;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.dataio.fileutils.ItemsWriterToJSONFile;
import org.janelia.colormipsearch.datarequests.ScoresFilter;
import org.janelia.colormipsearch.datarequests.SortCriteria;
import org.janelia.colormipsearch.datarequests.SortDirection;
import org.janelia.colormipsearch.dto.AbstractNeuronMetadata;
import org.janelia.colormipsearch.dto.CDMatchedTarget;
import org.janelia.colormipsearch.dto.EMNeuronMetadata;
import org.janelia.colormipsearch.dto.LMNeuronMetadata;
import org.janelia.colormipsearch.dto.ResultMatches;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.CDMatchEntity;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.janelia.colormipsearch.results.MatchResultsGrouping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LMCDMatchesExporter extends AbstractCDMatchesExporter {
    private static final Logger LOG = LoggerFactory.getLogger(EMCDMatchesExporter.class);

    public LMCDMatchesExporter(CachedJacsDataHelper jacsDataHelper,
                               DataSourceParam dataSourceParam,
                               ScoresFilter scoresFilter,
                               Path outputDir,
                               NeuronMatchesReader<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> neuronMatchesReader,
                               ItemsWriterToJSONFile resultMatchesWriter,
                               int processingPartitionSize) {
        super(jacsDataHelper, dataSourceParam, scoresFilter, outputDir, neuronMatchesReader, resultMatchesWriter, processingPartitionSize);
    }

    @Override
    public void runExport() {
        List<String> targets = neuronMatchesReader.listMatchesLocations(Collections.singletonList(dataSourceParam));
        ItemsHandling.partitionCollection(targets, processingPartitionSize).entrySet().stream().parallel()
                .forEach(indexedPartition -> {
                    indexedPartition.getValue().forEach(targetId -> {
                        LOG.info("Read color depth matches for {}", targetId);
                        List<CDMatchEntity<?, ?>> matchesForTarget = neuronMatchesReader.readMatchesForTargets(
                                dataSourceParam.getAlignmentSpace(),
                                dataSourceParam.getLibraryName(),
                                Collections.singletonList(targetId),
                                scoresFilter,
                                null, // use the tags for selecting the masks but not for selecting the matches
                                Collections.singletonList(
                                        new SortCriteria("normalizedScore", SortDirection.DESC)
                                ));
                        LOG.info("Write color depth matches for {}", targetId);
                        writeResults(matchesForTarget);
                    });
                });
    }

    private <M extends EMNeuronMetadata, T extends LMNeuronMetadata> void
    writeResults(List<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> matches) {
        // group results by target MIP ID
        List<Function<T, ?>> grouping = Collections.singletonList(
                AbstractNeuronMetadata::getMipId
        );
        // order descending by normalized score
        Comparator<CDMatchedTarget<M>> ordering = Comparator.comparingDouble(m -> -m.getNormalizedScore());
        List<ResultMatches<T, CDMatchedTarget<M>>> groupedMatches = MatchResultsGrouping.groupByTarget(
                matches,
                grouping,
                ordering);
        // retrieve source ColorDepth MIPs
        retrieveAllCDMIPs(matches);
        // update all neuron from all grouped matches
        groupedMatches.forEach(m -> updateMatchedResultsMetadata(m,
                this::updateLMNeuron,
                this::updateEMNeuron
        ));
        // write results by target MIP ID
        resultMatchesWriter.writeGroupedItemsList(groupedMatches, AbstractNeuronMetadata::getMipId, outputDir);
    }
}
