package org.janelia.colormipsearch.cmd.dataexport;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.cmd.jacsdata.CDMIPSample;
import org.janelia.colormipsearch.cmd.jacsdata.CachedJacsDataHelper;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.dataio.fileutils.ItemsWriterToJSONFile;
import org.janelia.colormipsearch.datarequests.ScoresFilter;
import org.janelia.colormipsearch.datarequests.SortCriteria;
import org.janelia.colormipsearch.datarequests.SortDirection;
import org.janelia.colormipsearch.dto.AbstractNeuronMetadata;
import org.janelia.colormipsearch.dto.EMNeuronMetadata;
import org.janelia.colormipsearch.dto.LMNeuronMetadata;
import org.janelia.colormipsearch.dto.LMPPPNeuronMetadata;
import org.janelia.colormipsearch.dto.PPPMatchedTarget;
import org.janelia.colormipsearch.dto.ResultMatches;
import org.janelia.colormipsearch.model.AbstractMatchEntity;
import org.janelia.colormipsearch.model.EMNeuronEntity;
import org.janelia.colormipsearch.model.FileType;
import org.janelia.colormipsearch.model.Gender;
import org.janelia.colormipsearch.model.LMNeuronEntity;
import org.janelia.colormipsearch.model.PPPMatchEntity;
import org.janelia.colormipsearch.model.PublishedLMImage;
import org.janelia.colormipsearch.model.PublishedURLs;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.janelia.colormipsearch.results.MatchResultsGrouping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EMPPPMatchesExporter extends AbstractDataExporter {
    private static final Logger LOG = LoggerFactory.getLogger(EMCDMatchesExporter.class);

    private final Map<String, Set<String>> publishedAlignmentSpaceAliases;
    private final ScoresFilter scoresFilter;
    private final NeuronMatchesReader<PPPMatchEntity<EMNeuronEntity, LMNeuronEntity>> neuronMatchesReader;
    private final ItemsWriterToJSONFile resultMatchesWriter;
    private final int processingPartitionSize;

    public EMPPPMatchesExporter(CachedJacsDataHelper jacsDataHelper,
                                DataSourceParam dataSourceParam,
                                Map<String, Set<String>> publishedAlignmentSpaceAliases,
                                ScoresFilter scoresFilter,
                                int relativesUrlsToComponent,
                                Path outputDir,
                                Executor executor,
                                NeuronMatchesReader<PPPMatchEntity<EMNeuronEntity, LMNeuronEntity>> neuronMatchesReader,
                                ItemsWriterToJSONFile resultMatchesWriter,
                                int processingPartitionSize) {
        super(jacsDataHelper, dataSourceParam, relativesUrlsToComponent, outputDir, executor);
        this.publishedAlignmentSpaceAliases = publishedAlignmentSpaceAliases;
        this.scoresFilter = scoresFilter;
        this.neuronMatchesReader = neuronMatchesReader;
        this.resultMatchesWriter = resultMatchesWriter;
        this.processingPartitionSize = processingPartitionSize;
    }

    @Override
    public void runExport() {
        long startProcessingTime = System.currentTimeMillis();
        List<String> masks = neuronMatchesReader.listMatchesLocations(Collections.singletonList(dataSourceParam));
        List<CompletableFuture<Void>> allExportsJobs = ItemsHandling.partitionCollection(masks, processingPartitionSize).entrySet().stream().parallel()
                .map(indexedPartition -> CompletableFuture.<Void>supplyAsync(() -> {
                    runExportForMaskIds(indexedPartition.getKey(), indexedPartition.getValue());
                    return null;
                }, executor))
                .collect(Collectors.toList());
        CompletableFuture.allOf(allExportsJobs.toArray(new CompletableFuture<?>[0])).join();
        LOG.info("Finished all exports in {}s", (System.currentTimeMillis()-startProcessingTime)/1000.);
    }

    private void runExportForMaskIds(int jobId, List<String> maskIds) {
        long startProcessingTime = System.currentTimeMillis();
        LOG.info("Start processing {} masks from partition {}", maskIds.size(), jobId);
        maskIds.forEach(maskId -> {
            LOG.info("Read PPP matches for {}", maskId);
            List<PPPMatchEntity<EMNeuronEntity, LMNeuronEntity>> allMatchesForMask = neuronMatchesReader.readMatchesForMasks(
                    dataSourceParam.getAlignmentSpace(),
                    dataSourceParam.getLibraries(),
                    Collections.singletonList(maskId),
                    scoresFilter,
                    null, // use the tags for selecting the masks but not for selecting the matches
                    Collections.singletonList(
                            new SortCriteria("rank", SortDirection.ASC)
                    ));
            LOG.info("Filter out PPP matches without any images for {}", maskId);
            List<PPPMatchEntity<EMNeuronEntity, LMNeuronEntity>> matchesForMask = allMatchesForMask.stream()
                    .filter(PPPMatchEntity::hasSourceImageFiles)
                    .collect(Collectors.toList());
            LOG.info("Prepare writing {} PPPM results for {} out of {}",
                    matchesForMask.size(), maskId, allMatchesForMask.size());
            prepareAndWriteResults(matchesForMask);
        });
        LOG.info("Finished processing partition {} in {}s", jobId, (System.currentTimeMillis()-startProcessingTime)/1000.);
    }

    private void
    prepareAndWriteResults(List<PPPMatchEntity<EMNeuronEntity, LMNeuronEntity>> matches) {
        // group results by mask's published name
        List<Function<EMNeuronMetadata, ?>> grouping = Collections.singletonList(
                AbstractNeuronMetadata::getPublishedName
        );
        // order ascending by rank
        Comparator<PPPMatchedTarget<LMNeuronMetadata>> ordering = Comparator.comparingDouble(PPPMatchedTarget::getRank);
        List<ResultMatches<EMNeuronMetadata, PPPMatchedTarget<LMNeuronMetadata>>> groupedMatches = MatchResultsGrouping.groupByMask(
                matches,
                grouping,
                ordering);

        // retrieve source data
        Map<String, List<PublishedLMImage>> lmPublishedImages = retrieveEMAndLMSourceData(matches);
        Map<Number, PublishedURLs> indexedPublishedURLs = jacsDataHelper.retrievePublishedURLs(
                matches.stream().map(AbstractMatchEntity::getMaskImage).collect(Collectors.toSet())
        );
        // update grouped matches
        groupedMatches.forEach(r -> updateMatchedResultsMetadata(r, lmPublishedImages, indexedPublishedURLs));
        // write results by mask (EM) ref ID (this is actually JACS EMBodyRef ID)
        resultMatchesWriter.writeGroupedItemsList(groupedMatches, EMNeuronMetadata::getEmRefId, outputDir);
    }

    /**
     * Retrieve EM color depth MIPs and LM published images for the given PPP matches
     *
     * @param matches for which to retrieve source data
     * @return LM published images indexed by Sample reference.
     */
    private Map<String, List<PublishedLMImage>> retrieveEMAndLMSourceData(List<PPPMatchEntity<EMNeuronEntity, LMNeuronEntity>> matches) {
        jacsDataHelper.retrieveCDMIPs(matches.stream()
                .flatMap(m -> Stream.of(m.getMaskMIPId(), m.getMatchedMIPId()))
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toSet()));
        Map<String, CDMIPSample> retrievedSamples = jacsDataHelper.retrieveLMSamples(matches.stream().map(PPPMatchEntity::extractLMSampleName).collect(Collectors.toSet()));
        return jacsDataHelper.retrievePublishedImages(
                null,
                retrievedSamples.values().stream().map(CDMIPSample::sampleRef).collect(Collectors.toSet()));
    }

    private void updateMatchedResultsMetadata(ResultMatches<EMNeuronMetadata, PPPMatchedTarget<LMNeuronMetadata>> resultMatches,
                                              Map<String, List<PublishedLMImage>> lmPublishedImages,
                                              Map<Number, PublishedURLs> publishedURLsMap) {
        updateEMNeuron(resultMatches.getKey(), publishedURLsMap.get(resultMatches.getKey().getInternalId()));
        resultMatches.getKey().transformAllNeuronFiles(this::relativizeURL);
        resultMatches.getItems().forEach(m -> updateTargetFromLMSample(resultMatches.getKey(), m, lmPublishedImages));
    }

    private void updateTargetFromLMSample(EMNeuronMetadata emNeuron,
                                          PPPMatchedTarget<LMNeuronMetadata> pppMatch,
                                          Map<String, List<PublishedLMImage>> lmPublishedImages) {
        LMPPPNeuronMetadata lmNeuron;
        if (pppMatch.getTargetImage() == null) {
            lmNeuron = new LMPPPNeuronMetadata();
            // copy the alignment space and anatomical area from the EM neuron
            lmNeuron.setAlignmentSpace(emNeuron.getAlignmentSpace());
            lmNeuron.setAnatomicalArea(emNeuron.getAnatomicalArea());
            lmNeuron.setObjective(pppMatch.getSourceObjective());
            pppMatch.setTargetImage(lmNeuron);
        } else {
            lmNeuron = new LMPPPNeuronMetadata(pppMatch.getTargetImage());
        }
        lmNeuron.setLibraryName(jacsDataHelper.getLibraryName(pppMatch.getSourceLmLibrary()));
        CDMIPSample sample = jacsDataHelper.getLMSample(pppMatch.getSourceLmName());
        if (sample != null) {
            String lm3DStackURL = findPublishedLM3DStack(
                    sample.sampleRef(),
                    lmNeuron.getAlignmentSpace(),
                    lmPublishedImages);
            lmNeuron.setSampleId(sample.id);
            lmNeuron.setPublishedName(sample.lmLineName());
            lmNeuron.setSlideCode(sample.slideCode);
            lmNeuron.setGender(Gender.fromVal(sample.gender));
            lmNeuron.setMountingProtocol(sample.mountingProtocol);
            lmNeuron.setNeuronFile(FileType.VisuallyLosslessStack, relativizeURL(lm3DStackURL));
            // collect updated match files
            Map<FileType, String> updatedMatchFiles = new LinkedHashMap<>();
            pppMatch.getMatchFiles()
                    .forEach((ft, fn) -> {
                        String updatedFN = fn.replace("{lmLine}", sample.lmLineName())
                                .replace("{lmSlideCode}", sample.slideCode);
                        updatedMatchFiles.put(ft, updatedFN);
                    });
            // then replace them just to be safe that we are not updating what we're reading
            updatedMatchFiles.forEach((ft, fn) -> pppMatch.setMatchFile(ft, relativizeURL(fn)));
        } else {
            LOG.error("No sample found for {}", pppMatch.getSourceLmName());
        }
    }

    private String findPublishedLM3DStack(String sampleRef,
                                          String alignmentSpace,
                                          Map<String, List<PublishedLMImage>> lmPublishedImages) {
        if (lmPublishedImages.containsKey(sampleRef)) {
            Set<String> aliasesForAlignmentSpace = publishedAlignmentSpaceAliases.getOrDefault(alignmentSpace, Collections.emptySet());
            return lmPublishedImages.get(sampleRef).stream()
                    .filter(pi -> pi.getAlignmentSpace().equals(alignmentSpace) ||
                            (CollectionUtils.isNotEmpty(aliasesForAlignmentSpace) && aliasesForAlignmentSpace.contains(pi.getAlignmentSpace())))
                    .filter(pi -> pi.hasFile("VisuallyLosslessStack"))
                    .findFirst()
                    .map(pi -> pi.getFile("VisuallyLosslessStack"))
                    .orElse(null);
        } else {
            return null;
        }
    }
}
