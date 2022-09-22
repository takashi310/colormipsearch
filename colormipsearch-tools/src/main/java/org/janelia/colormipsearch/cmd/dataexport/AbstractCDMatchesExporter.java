package org.janelia.colormipsearch.cmd.dataexport;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.RegExUtils;
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
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.FileType;
import org.janelia.colormipsearch.model.PublishedURLs;

public abstract class AbstractCDMatchesExporter extends AbstractDataExporter {
    final ScoresFilter scoresFilter;
    final NeuronMatchesReader<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> neuronMatchesReader;
    final ItemsWriterToJSONFile resultMatchesWriter;
    final int processingPartitionSize;

    protected AbstractCDMatchesExporter(CachedJacsDataHelper jacsDataHelper,
                                        DataSourceParam dataSourceParam,
                                        ScoresFilter scoresFilter,
                                        int relativesUrlsToComponent,
                                        Path outputDir,
                                        NeuronMatchesReader<CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> neuronMatchesReader,
                                        ItemsWriterToJSONFile resultMatchesWriter,
                                        int processingPartitionSize) {
        super(jacsDataHelper, dataSourceParam, relativesUrlsToComponent, outputDir);
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
                                 BiConsumer<M, PublishedURLs> updateKeyMethod,
                                 BiConsumer<T, PublishedURLs> updateTargetMatchMethod,
                                 Map<Number, PublishedURLs> publisheURLsByNeuronId) {
        updateKeyMethod.accept(resultMatches.getKey(), publisheURLsByNeuronId.get(resultMatches.getKey().getInternalId()));
        resultMatches.getKey().transformAllNeuronFiles(this::relativizeURL);
        String maskSourceImageName = resultMatches.getKey().getNeuronComputeFile(ComputeFileType.SourceColorDepthImage);
        String maskMipImageName = resultMatches.getKey().getNeuronFile(FileType.ColorDepthMip);
        resultMatches.getItems().forEach(target -> {
            updateTargetMatchMethod.accept(target.getTargetImage(), publisheURLsByNeuronId.get(target.getTargetImage().getInternalId()));
            target.getTargetImage().transformAllNeuronFiles(this::relativizeURL);
            // update match files
            String maskInputImageName = target.getMatchFile(FileType.ColorDepthMipInput);
            String targetInputImageName = target.getMatchFile(FileType.ColorDepthMipMatch);
            String targetSourceImageName = target.getTargetImage().getNeuronComputeFile(ComputeFileType.SourceColorDepthImage);
            String targetMipImageName = target.getTargetImage().getNeuronFile(FileType.ColorDepthMip);
            target.setMatchFile(FileType.ColorDepthMipInput, getMIPFileName(maskInputImageName, maskSourceImageName, maskMipImageName));
            target.setMatchFile(FileType.ColorDepthMipMatch, getMIPFileName(targetInputImageName, targetSourceImageName, targetMipImageName));
        });
    }

    /**
     * This creates the corresponding display name for the input MIP.
     * To do that it finds the suffix that was used to create the inputImageName from the sourceImageName and appends it to the mipImageName.
     *
     * @param inputImageFileName
     * @param sourceImageFileName
     * @param mipImageFileName
     * @return
     */
    private String getMIPFileName(String inputImageFileName, String sourceImageFileName, String mipImageFileName) {
        if (StringUtils.isNotBlank(mipImageFileName) &&
                StringUtils.isNotBlank(sourceImageFileName) &&
                StringUtils.isNotBlank(inputImageFileName)) {
            String sourceName = RegExUtils.replacePattern(
                    new File(sourceImageFileName).getName(),
                    "(_)?(CDM)?\\..*$", ""); // clear  _CDM.<ext> suffix
            String inputName = RegExUtils.replacePattern(
                    new File(inputImageFileName).getName(),
                    "(_)?(CDM)?\\..*$", ""); // clear  _CDM.<ext> suffix
            String imageSuffix = RegExUtils.replacePattern(
                    StringUtils.removeStart(inputName, sourceName), // typically the segmentation name shares the same prefix with the original mip name
                    "^[-_]",
                    ""
            ); // remove the hyphen or underscore prefix
            String mipName = RegExUtils.replacePattern(new File(mipImageFileName).getName(),
                    "\\..*$", ""); // clear  .<ext> suffix
            return StringUtils.isBlank(imageSuffix)
                    ? mipName + ".png"
                    : mipName + "-" + imageSuffix + ".png";
        } else {
            return null;
        }
    }
}
