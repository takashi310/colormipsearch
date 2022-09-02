package org.janelia.colormipsearch.cmd.dataexport;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
                                 Consumer<M> updateKeyMethod,
                                 Consumer<T> updateTargetMatchMethod) {
        updateKeyMethod.accept(resultMatches.getKey());
        resultMatches.getKey().updateAllNeuronFiles(this::relativizeURL);
        String maskSourceImageName = resultMatches.getKey().getNeuronComputeFile(ComputeFileType.SourceColorDepthImage);
        String maskMipImageName = resultMatches.getKey().getNeuronFile(FileType.ColorDepthMip);
        resultMatches.getItems().forEach(target -> {
            updateTargetMatchMethod.accept(target.getTargetImage());
            target.getTargetImage().updateAllNeuronFiles(this::relativizeURL);
            String maskInputImageName = target.getMatchFile(FileType.ColorDepthMipInput);
            String targetInputImageName = target.getMatchFile(FileType.ColorDepthMipMatch);
            String targetSourceImageName = target.getTargetImage().getNeuronComputeFile(ComputeFileType.SourceColorDepthImage);
            String targetMipImageName = target.getTargetImage().getNeuronFile(FileType.ColorDepthMip);
            target.setMatchFile(FileType.ColorDepthMipInput, getMIPFileName(maskInputImageName, maskSourceImageName, maskMipImageName));
            target.setMatchFile(FileType.ColorDepthMipMatch, getMIPFileName(targetInputImageName, targetSourceImageName, targetMipImageName));
        });
    }

    private String getMIPFileName(String inputImageName, String sourceImageName, String mipImageName) {
        if (StringUtils.isNotBlank(mipImageName) && StringUtils.isNotBlank(sourceImageName) && StringUtils.isNotBlank(inputImageName)) {
            String sourceName = RegExUtils.replacePattern(sourceImageName, "(_)?(CDM)?\\..*$", ""); // clear  _CDM.<ext> suffix
            String inputName = RegExUtils.replacePattern(inputImageName, "(_)?(CDM)?\\..*$", ""); // clear  _CDM.<ext> suffix
            String imageSuffix = RegExUtils.replacePattern(
                    StringUtils.removeStart(inputName, sourceName), // typically the segmentation name shares the same prefix with the original mip name
                    "^[-_]",
                    ""
            ); // remove the hyphen or underscore prefix
            String mipName = RegExUtils.replacePattern(mipImageName, "\\..*$", ""); // clear  .<ext> suffix
            return StringUtils.isBlank(imageSuffix)
                    ? mipName + ".png"
                    : mipName + "-" + imageSuffix + ".png";
        } else {
            return null;
        }
    }
//    @SuppressWarnings("unchecked")
//    private void updateTagAndNeuronFiles(InputCDMipNeuron<? extends AbstractNeuronEntity> cdmip) {
//        cdmip.getNeuronMetadata().addTag(args.tag);
//        if (cdmip.getNeuronMetadata().hasComputeFile(ComputeFileType.InputColorDepthImage) &&
//                cdmip.getNeuronMetadata().hasNeuronFile(FileType.ColorDepthMip)) {
//            // ColorDepthInput filename must be expressed in terms of publishedName (not internal name),
//            //  and must include the integer suffix that identifies exactly which image it is (for LM),
//            //  when there are multiple images for a given combination of parameters
//            // in practical terms, we take the filename from imageURL, which has
//            //  the publishedName in it, and graft on the required integer from imageName (for LM), which
//            //  has the internal name; we have to similarly grab _FL from EM names
//
//            // remove directories and extension (which we know is ".png") from imageURL:
//            Path imagePath = Paths.get(cdmip.getNeuronMetadata().getNeuronFile(FileType.ColorDepthMip));
//            String colorDepthInputName = createColorDepthInputName(
//                    Paths.get(cdmip.getNeuronMetadata().getComputeFileName(ComputeFileType.SourceColorDepthImage)).getFileName().toString(),
//                    Paths.get(cdmip.getNeuronMetadata().getComputeFileName(ComputeFileType.InputColorDepthImage)).getFileName().toString(),
//                    imagePath.getFileName().toString());
//            cdmip.getNeuronMetadata().setNeuronFile(FileType.ColorDepthMipInput, colorDepthInputName);
//        }
//    }
//
//    /**
//     * Create the published name for the input image - the one that will actually be "color depth searched".
//     *
//     * @param mipFileName
//     * @param imageFileName
//     * @param displayFileName
//     * @return
//     */
//    private String createColorDepthInputName(String mipFileName, String imageFileName, String displayFileName) {
//        String mipName = RegExUtils.replacePattern(mipFileName, "(_)?(CDM)?\\..*$", ""); // clear  _CDM.<ext> suffix
//        String imageName = RegExUtils.replacePattern(imageFileName, "(_)?(CDM)?\\..*$", ""); // clear  _CDM.<ext> suffix
//        String imageSuffix = RegExUtils.replacePattern(
//                StringUtils.removeStart(imageName, mipName), // typically the segmentation name shares the same prefix with the original mip name
//                "^[-_]",
//                ""
//        ); // remove the hyphen or underscore prefix
//        String displayName = RegExUtils.replacePattern(displayFileName, "\\..*$", ""); // clear  .<ext> suffix
//        return StringUtils.isBlank(imageSuffix)
//                ? displayName + ".png"
//                : displayName + "-" + imageSuffix + ".png";
//    }

}
