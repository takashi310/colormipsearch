package org.janelia.colormipsearch.cmd.dataexport;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.janelia.colormipsearch.model.PublishedURLs;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.janelia.colormipsearch.results.MatchResultsGrouping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EMCDMatchesExporter extends AbstractCDMatchesExporter {
    private static final Logger LOG = LoggerFactory.getLogger(EMCDMatchesExporter.class);

    public EMCDMatchesExporter(CachedJacsDataHelper jacsDataHelper,
                               DataSourceParam dataSourceParam,
                               ScoresFilter scoresFilter,
                               int relativesUrlsToComponent,
                               Path outputDir,
                               NeuronMatchesReader<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> neuronMatchesReader,
                               ItemsWriterToJSONFile resultMatchesWriter,
                               int processingPartitionSize) {
        super(jacsDataHelper, dataSourceParam, scoresFilter, relativesUrlsToComponent, outputDir, neuronMatchesReader, resultMatchesWriter, processingPartitionSize);
    }

    @Override
    public void runExport() {
        List<String> masks = neuronMatchesReader.listMatchesLocations(Collections.singletonList(dataSourceParam));
        ItemsHandling.partitionCollection(masks, processingPartitionSize)
                .entrySet().stream().parallel()
                .forEach(indexedPartition -> {
                    long startProcessingTime = System.currentTimeMillis();
                    LOG.info("Start processing partition {}", indexedPartition.getKey());
                    indexedPartition.getValue().forEach(maskId -> {
                        LOG.info("Read EM color depth matches for {}", maskId);
                        List<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> allMatchesForMask = neuronMatchesReader.readMatchesForMasks(
                                dataSourceParam.getAlignmentSpace(),
                                null, // no mask library specified - we use mask MIP
                                Collections.singletonList(maskId),
                                scoresFilter,
                                null, // use the tags for selecting the masks but not for selecting the matches
                                null // no sorting because it uses too much memory on the server
                        );
                        LOG.info("Select best EM matches for {} out of {} matches", maskId, allMatchesForMask.size());
                        List<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> selectedMatchesForMask =
                                selectBestMatchPerMIPPair(allMatchesForMask);
                        LOG.info("Write {} color depth matches for {}", selectedMatchesForMask.size(), maskId);
                        writeResults(selectedMatchesForMask);
                    });
                    LOG.info("Finished processing partition {} in {}s", indexedPartition.getKey(), (System.currentTimeMillis()-startProcessingTime)/1000.);
                });
    }

    private <M extends EMNeuronMetadata, T extends LMNeuronMetadata> void
    writeResults(List<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> matches) {
        // group results by mask MIP ID
        List<Function<M, ?>> grouping = Collections.singletonList(
                AbstractNeuronMetadata::getMipId
        );
        // order descending by normalized score
        Comparator<CDMatchedTarget<T>> ordering = Comparator.comparingDouble(m -> -m.getNormalizedScore());
        List<ResultMatches<M, CDMatchedTarget<T>>> groupedMatches = MatchResultsGrouping.groupByMask(
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
        List<ResultMatches<M, CDMatchedTarget<T>>> publishedMatches = groupedMatches.stream()
                .peek(m -> updateMatchedResultsMetadata(m,
                        this::updateEMNeuron,
                        this::updateLMNeuron,
                        indexedNeuronURLs
                ))
                .filter(resultMatches -> resultMatches.getKey().isPublished()) // filter out unpublished EMs
                .peek(resultMatches -> resultMatches.setItems(resultMatches.getItems().stream()
                        .filter(m -> m.getTargetImage().isPublished()) // filter out unpublished LMs
                        .collect(Collectors.toList())))
                .collect(Collectors.toList());
        // write results by mask MIP ID
        resultMatchesWriter.writeGroupedItemsList(publishedMatches, AbstractNeuronMetadata::getMipId, outputDir);
    }
}
