package org.janelia.colormipsearch.cmd.dataexport;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.cmd.jacsdata.CachedJacsDataHelper;
import org.janelia.colormipsearch.cmd.jacsdata.ColorDepthMIP;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.dataio.fileutils.ItemsWriterToJSONFile;
import org.janelia.colormipsearch.datarequests.ScoresFilter;
import org.janelia.colormipsearch.dto.AbstractNeuronMetadata;
import org.janelia.colormipsearch.dto.CDMatchedTarget;
import org.janelia.colormipsearch.dto.ResultMatches;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.CDMatchEntity;

public abstract class AbstractCDMatchesExporter extends AbstractDataExporter {
    final ScoresFilter scoresFilter;
    final NeuronMatchesReader<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> neuronMatchesReader;
    final ItemsWriterToJSONFile resultMatchesWriter;
    final int processingPartitionSize;

    protected AbstractCDMatchesExporter(CachedJacsDataHelper jacsDataHelper,
                                        DataSourceParam dataSourceParam,
                                        ScoresFilter scoresFilter,
                                        Path outputDir,
                                        NeuronMatchesReader<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> neuronMatchesReader,
                                        ItemsWriterToJSONFile resultMatchesWriter,
                                        int processingPartitionSize) {
        super(jacsDataHelper, dataSourceParam, outputDir);
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
                .collect(Collectors.toMap(
                        m -> ImmutablePair.of(m.getMaskMIPId(), m.getMatchedMIPId()),
                        m -> m,
                        (m1, m2) -> m1.getNormalizedScore() >= m2.getNormalizedScore() ? m1 : m2)); // resolve by picking the best match
        return new ArrayList<>(bestMatchesPerMIPsPairs.values());
    }

    <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> void
    updateMatchedResultsMetadata(ResultMatches<M, CDMatchedTarget<T>> resultMatches,
                                 Consumer<M> updateKeyMethod,
                                 Consumer<T> updateTargetMatchMethod) {
        updateKeyMethod.accept(resultMatches.getKey());
        resultMatches.getItems().forEach(target -> {
            updateTargetMatchMethod.accept(target.getTargetImage());
        });
    }
}
