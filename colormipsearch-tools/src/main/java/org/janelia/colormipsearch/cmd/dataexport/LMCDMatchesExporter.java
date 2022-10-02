package org.janelia.colormipsearch.cmd.dataexport;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.janelia.colormipsearch.cmd.jacsdata.CachedJacsDataHelper;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.dataio.fileutils.ItemsWriterToJSONFile;
import org.janelia.colormipsearch.datarequests.ScoresFilter;
import org.janelia.colormipsearch.dto.AbstractNeuronMetadata;
import org.janelia.colormipsearch.dto.CDMatchedTarget;
import org.janelia.colormipsearch.dto.EMNeuronMetadata;
import org.janelia.colormipsearch.dto.LMNeuronMetadata;
import org.janelia.colormipsearch.dto.ResultMatches;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.CDMatchEntity;
import org.janelia.colormipsearch.model.PublishedURLs;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.janelia.colormipsearch.results.MatchResultsGrouping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LMCDMatchesExporter extends AbstractCDMatchesExporter {
    private static final Logger LOG = LoggerFactory.getLogger(EMCDMatchesExporter.class);

    public LMCDMatchesExporter(CachedJacsDataHelper jacsDataHelper,
                               DataSourceParam dataSourceParam,
                               ScoresFilter scoresFilter,
                               int relativesUrlsToComponent,
                               ImageStoreMapping imageStoreMapping,
                               Path outputDir,
                               Executor executor,
                               NeuronMatchesReader<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> neuronMatchesReader,
                               ItemsWriterToJSONFile resultMatchesWriter,
                               int processingPartitionSize) {
        super(jacsDataHelper, dataSourceParam, scoresFilter, relativesUrlsToComponent, imageStoreMapping, outputDir, executor, neuronMatchesReader, resultMatchesWriter, processingPartitionSize);
    }

    @Override
    public void runExport() {
        long startProcessingTime = System.currentTimeMillis();
        List<String> targets = neuronMatchesReader.listMatchesLocations(Collections.singletonList(dataSourceParam));
        List<CompletableFuture<Void>> allExportsJobs = ItemsHandling.partitionCollection(targets, processingPartitionSize)
                .entrySet().stream().parallel()
                .map(indexedPartition -> CompletableFuture.<Void>supplyAsync(() -> {
                    runExportForTargetIds(indexedPartition.getKey(), indexedPartition.getValue());
                    return null;
                }, executor))
                .collect(Collectors.toList());
        CompletableFuture.allOf(allExportsJobs.toArray(new CompletableFuture<?>[0])).join();
        LOG.info("Finished all exports in {}s", (System.currentTimeMillis()-startProcessingTime)/1000.);
    }

    private void runExportForTargetIds(int jobId, List<String> targetIds) {
        long startProcessingTime = System.currentTimeMillis();
        LOG.info("Start processing {} targets from partition {}", targetIds.size(), jobId);
        targetIds.forEach(targetId -> {
            LOG.info("Read LM color depth matches for {}", targetId);
            List<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> allMatchesForTarget = neuronMatchesReader.readMatchesForTargets(
                    dataSourceParam.getAlignmentSpace(),
                    null, // no target library specified - we use target MIP
                    Collections.singletonList(targetId),
                    scoresFilter,
                    null, // use the tags for selecting the masks but not for selecting the matches
                    null // no sorting yet because it uses too much memory on the server
            );
            LOG.info("Select best LM matches for {} out of {} matches", targetId, allMatchesForTarget.size());
            List<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> selectedMatchesForTarget =
                    selectBestMatchPerMIPPair(allMatchesForTarget);
            LOG.info("Write {} color depth matches for {}", selectedMatchesForTarget.size(), targetId);
            writeResults(selectedMatchesForTarget);
        });
        LOG.info("Finished processing partition {} in {}s", jobId, (System.currentTimeMillis() - startProcessingTime) / 1000.);
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
        Map<Number, PublishedURLs> indexedNeuronURLs = jacsDataHelper.retrievePublishedURLs(
                matches.stream()
                        .flatMap(m -> Stream.of(m.getMaskImage(), m.getMatchedImage()))
                        .collect(Collectors.toSet())
        );
        LOG.info("Fill in missing info for {} matches", matches.size());
        // update all neuron from all grouped matches
        List<ResultMatches<T, CDMatchedTarget<M>>> publishedMatches = groupedMatches.stream()
                .peek(m -> updateMatchedResultsMetadata(m,
                        this::updateLMNeuron,
                        this::updateEMNeuron,
                        indexedNeuronURLs
                ))
                .filter(resultMatches -> resultMatches.getKey().isPublished()) // filter out unpublished LMs
                .peek(resultMatches -> resultMatches.setItems(resultMatches.getItems().stream()
                        .filter(m -> m.getTargetImage().isPublished()) // filter out unpublished EMs
                        .collect(Collectors.toList())))
                .collect(Collectors.toList());
        // write results by target MIP ID
        resultMatchesWriter.writeGroupedItemsList(publishedMatches, AbstractNeuronMetadata::getMipId, outputDir);
    }
}
