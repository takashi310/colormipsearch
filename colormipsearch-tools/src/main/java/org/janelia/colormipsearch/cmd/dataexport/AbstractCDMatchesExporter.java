package org.janelia.colormipsearch.cmd.dataexport;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.cmd.jacsdata.CachedJacsDataHelper;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.dataio.fileutils.ItemsWriterToJSONFile;
import org.janelia.colormipsearch.datarequests.ScoresFilter;
import org.janelia.colormipsearch.dto.AbstractNeuronMetadata;
import org.janelia.colormipsearch.dto.CDMatchedTarget;
import org.janelia.colormipsearch.dto.ResultMatches;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.CDMatchEntity;
import org.janelia.colormipsearch.model.FileType;
import org.janelia.colormipsearch.model.PublishedURLs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractCDMatchesExporter extends AbstractDataExporter {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractCDMatchesExporter.class);
    private static final Pattern SUSPICIOUS_MATCH_PATTERN = Pattern.compile("Suspicious match from .+ import");
    final ScoresFilter scoresFilter;
    final NeuronMatchesReader<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> neuronMatchesReader;
    final ItemsWriterToJSONFile resultMatchesWriter;
    final int processingPartitionSize;

    protected AbstractCDMatchesExporter(CachedJacsDataHelper jacsDataHelper,
                                        DataSourceParam dataSourceParam,
                                        ScoresFilter scoresFilter,
                                        int relativesUrlsToComponent,
                                        Path outputDir,
                                        Executor executor,
                                        NeuronMatchesReader<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> neuronMatchesReader,
                                        ItemsWriterToJSONFile resultMatchesWriter,
                                        int processingPartitionSize) {
        super(jacsDataHelper, dataSourceParam, relativesUrlsToComponent, outputDir, executor);
        this.scoresFilter = scoresFilter;
        this.neuronMatchesReader = neuronMatchesReader;
        this.resultMatchesWriter = resultMatchesWriter;
        this.processingPartitionSize = processingPartitionSize;
    }

    void retrieveAllCDMIPs(List<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> matches) {
        // retrieve source ColorDepth MIPs
        Set<String> sourceMIPIds = matches.stream()
                .flatMap(m -> Stream.of(m.getMaskMIPId(), m.getMatchedMIPId()))
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toSet());
        jacsDataHelper.retrieveCDMIPs(sourceMIPIds);
    }

    /**
     * Select the best matches for each pair of mip IDs
     */
    <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity> List<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> selectBestMatchPerMIPPair(
            List<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> cdMatchEntities) {
        Map<Pair<String, String>, CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> bestMatchesPerMIPsPairs = cdMatchEntities.stream()
                .filter(this::doesNotLookSuspicious)
                .collect(Collectors.toMap(
                        m -> ImmutablePair.of(m.getMaskMIPId(), m.getMatchedMIPId()),
                        m -> m,
                        (m1, m2) -> m1.getNormalizedScore() >= m2.getNormalizedScore() ? m1 : m2)); // resolve by picking the best match
        return new ArrayList<>(bestMatchesPerMIPsPairs.values());
    }

    /**
     * Check if the match was marked as suspicious at the time of import. That happens if one of the neurons,
     * either the mask or the target, did not exist at the time of import and it was artificially created.
     *
     * @param m
     * @return
     */
    private boolean doesNotLookSuspicious(CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity> m) {
        return m.getTags().stream().noneMatch(t -> SUSPICIOUS_MATCH_PATTERN.matcher(t).find());
    }

    <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> void
    updateMatchedResultsMetadata(ResultMatches<M, CDMatchedTarget<T>> resultMatches,
                                 BiConsumer<M, PublishedURLs> updateKeyMethod,
                                 BiConsumer<T, PublishedURLs> updateTargetMatchMethod,
                                 Map<Number, PublishedURLs> publisheURLsByNeuronId) {
        updateKeyMethod.accept(resultMatches.getKey(), publisheURLsByNeuronId.get(resultMatches.getKey().getInternalId()));
        resultMatches.getKey().transformAllNeuronFiles(this::relativizeURL);
        resultMatches.getItems().forEach(target -> {
            updateTargetMatchMethod.accept(target.getTargetImage(), publisheURLsByNeuronId.get(target.getTargetImage().getInternalId()));
            target.getTargetImage().transformAllNeuronFiles(this::relativizeURL);
            // update match files
            PublishedURLs maskImageURLs = publisheURLsByNeuronId.get(target.getMaskImageInternalId());
            if (maskImageURLs != null) {
                target.setMatchFile(
                        FileType.ColorDepthMipInput,
                        relativizeURL(maskImageURLs.getURLFor("searchable_neurons", null)
                ));
            } else {
                LOG.error("No published URLs for match mask {}:{} -> {}", target.getMaskImageInternalId(), resultMatches.getKey(), target);
            }
            PublishedURLs targetImageURLs = publisheURLsByNeuronId.get(target.getTargetImage().getInternalId());
            if (targetImageURLs != null) {
                target.setMatchFile(
                        FileType.ColorDepthMipMatch,
                        relativizeURL(targetImageURLs.getURLFor("searchable_neurons", null)
                ));
            } else {
                LOG.error("No published URLs for match target {}:{} -> {}", target.getMaskImageInternalId(), resultMatches.getKey(), target);
            }
        });
    }
}
