package org.janelia.colormipsearch.cmd.dataexport;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.janelia.colormipsearch.dto.PPPMatchedTarget;
import org.janelia.colormipsearch.dto.ResultMatches;
import org.janelia.colormipsearch.model.EMNeuronEntity;
import org.janelia.colormipsearch.model.Gender;
import org.janelia.colormipsearch.model.LMNeuronEntity;
import org.janelia.colormipsearch.model.PPPMatchEntity;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.janelia.colormipsearch.results.MatchResultsGrouping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EMPPPMatchesExporter extends AbstractDataExporter {
    private static final Logger LOG = LoggerFactory.getLogger(PerMaskCDMatchesExporter.class);

    private final ScoresFilter scoresFilter;
    private final NeuronMatchesReader<PPPMatchEntity<EMNeuronEntity, LMNeuronEntity>> neuronMatchesReader;
    private final ItemsWriterToJSONFile resultMatchesWriter;
    private final int processingPartitionSize;

    public EMPPPMatchesExporter(CachedJacsDataHelper jacsDataHelper,
                                DataSourceParam dataSourceParam,
                                ScoresFilter scoresFilter,
                                Path outputDir,
                                NeuronMatchesReader<PPPMatchEntity<EMNeuronEntity, LMNeuronEntity>> neuronMatchesReader,
                                ItemsWriterToJSONFile resultMatchesWriter,
                                int processingPartitionSize) {
        super(jacsDataHelper, dataSourceParam, outputDir);
        this.scoresFilter = scoresFilter;
        this.neuronMatchesReader = neuronMatchesReader;
        this.resultMatchesWriter = resultMatchesWriter;
        this.processingPartitionSize = processingPartitionSize;
    }

    @Override
    public void runExport() {
        List<String> masks = neuronMatchesReader.listMatchesLocations(Collections.singletonList(dataSourceParam));
        ItemsHandling.partitionCollection(masks, processingPartitionSize).entrySet().stream().parallel()
                .forEach(indexedPartition -> {
                    indexedPartition.getValue().forEach(maskId -> {
                        LOG.info("Read PPP matches for {}", maskId);
                        List<PPPMatchEntity<EMNeuronEntity, LMNeuronEntity>> matchesForMask = neuronMatchesReader.readMatchesForMasks(
                                dataSourceParam.getAlignmentSpace(),
                                dataSourceParam.getLibraryName(),
                                Collections.singletonList(maskId),
                                scoresFilter,
                                null, // use the tags for selecting the masks but not for selecting the matches
                                Collections.singletonList(
                                        new SortCriteria("rank", SortDirection.ASC)
                                ));
                        LOG.info("Write PPP matches for {}", maskId);
                        writeResults(matchesForMask);
                    });
                });
    }

    private void
    writeResults(List<PPPMatchEntity<EMNeuronEntity, LMNeuronEntity>> matches) {
        // group results by mask's published name
        List<Function<EMNeuronMetadata, ?>> grouping = Collections.singletonList(
                AbstractNeuronMetadata::getPublishedName
        );
        // order ascending by rank
        Comparator<PPPMatchedTarget<LMNeuronMetadata>> ordering = Comparator.comparingDouble(PPPMatchedTarget::getRank);
        List<ResultMatches<EMNeuronMetadata, PPPMatchedTarget<LMNeuronMetadata>>> matchesByMask = MatchResultsGrouping.groupByMask(
                matches,
                grouping,
                ordering);
        // retrieve source data
        jacsDataHelper.retrieveCDMIPs(matches.stream()
                .flatMap(m -> Stream.of(m.getMaskMIPId(), m.getMatchedMIPId()))
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toSet()));
        jacsDataHelper.retrieveLMSamples(matches.stream().map(PPPMatchEntity::getSourceLmName).collect(Collectors.toSet()));
        // update grouped matches
        matchesByMask.forEach(this::updateMatchedResultsMetadata);
        // write results by mask (EM) published name
        resultMatchesWriter.writeGroupedItemsList(matchesByMask, AbstractNeuronMetadata::getPublishedName, outputDir);
    }

    private void
    updateMatchedResultsMetadata(ResultMatches<EMNeuronMetadata, PPPMatchedTarget<LMNeuronMetadata>> resultMatches) {
        updateEMNeuron(resultMatches.getKey());
        resultMatches.getItems().forEach(this::updateTargetFromLMSample);
    }

    private void updateTargetFromLMSample(PPPMatchedTarget<LMNeuronMetadata> pppMatch) {
        LMNeuronMetadata lmNeuron;
        if (pppMatch.getTargetImage() == null) {
            lmNeuron = new LMNeuronMetadata();
            pppMatch.setTargetImage(lmNeuron);
        } else {
            lmNeuron = pppMatch.getTargetImage();
        }
        lmNeuron.setLibraryName(jacsDataHelper.getLibraryName(pppMatch.getSourceLmLibrary()));
        CDMIPSample sample = jacsDataHelper.getLMSample(pppMatch.getSourceLmName());
        if (sample != null) {
            lmNeuron.setPublishedName(sample.line);
            lmNeuron.setSlideCode(sample.slideCode);
            lmNeuron.setGender(Gender.fromVal(sample.gender));
            lmNeuron.setDriver(sample.driver);
            lmNeuron.setMountingProtocol(sample.mountingProtocol);
        }
    }
}
