package org.janelia.colormipsearch.cmd.cdsprocess;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import org.janelia.colormipsearch.cds.ColorDepthSearchAlgorithm;
import org.janelia.colormipsearch.cds.ColorMIPSearch;
import org.janelia.colormipsearch.cds.PixelMatchScore;
import org.janelia.colormipsearch.cmd.CachedMIPsUtils;
import org.janelia.colormipsearch.imageprocessing.ImageArray;
import org.janelia.colormipsearch.mips.NeuronMIP;
import org.janelia.colormipsearch.mips.NeuronMIPUtils;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.FileType;
import org.janelia.colormipsearch.model.MatchComputeFileType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractColorMIPSearchProcessor<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> implements ColorMIPSearchProcessor<M, T> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractColorMIPSearchProcessor.class);

    final ColorMIPSearch colorMIPSearch;
    final int localProcessingPartitionSize;

    AbstractColorMIPSearchProcessor(ColorMIPSearch colorMIPSearch, int localProcessingPartitionSize) {
        this.colorMIPSearch = colorMIPSearch;
        this.localProcessingPartitionSize = localProcessingPartitionSize > 0 ? localProcessingPartitionSize : 1;
    }

    <N extends AbstractNeuronMetadata> Map<ComputeFileType, Supplier<ImageArray<?>>> getVariantImagesSuppliers(Set<ComputeFileType> variantTypes,
                                                                                                               N neuronMIP) {
        return NeuronMIPUtils.getImageLoaders(
                neuronMIP,
                variantTypes,
                (n, cft) -> NeuronMIPUtils.getImageArray(CachedMIPsUtils.loadMIP(n, cft)));
    }

    /**
     * Applies the given algorithm to find the pixel matches.
     *
     * @param cdsAlgorithm
     * @param maskImage
     * @param targetImage
     * @return null if no match was found otherwise it returns a @CDSMatch@
     */
    @Nonnull
    CDMatch<M, T> findPixelMatch(ColorDepthSearchAlgorithm<PixelMatchScore> cdsAlgorithm,
                                 NeuronMIP<M> maskImage,
                                 NeuronMIP<T> targetImage) {
        CDMatch<M, T> result = new CDMatch<>();
        result.setMaskImage(maskImage.getNeuronInfo());
        result.setMatchedImage(targetImage.getNeuronInfo());
        try {
            Map<ComputeFileType, Supplier<ImageArray<?>>> variantImageSuppliers =
                    getVariantImagesSuppliers(cdsAlgorithm.getRequiredTargetVariantTypes(), targetImage.getNeuronInfo());
            PixelMatchScore pixelMatchScore = cdsAlgorithm.calculateMatchingScore(
                    NeuronMIPUtils.getImageArray(targetImage),
                    variantImageSuppliers);
            result.setMatchFound(colorMIPSearch.isMatch(pixelMatchScore));
            result.setMatchingPixels(pixelMatchScore.getScore());
            result.setMatchingPixelsRatio(pixelMatchScore.getNormalizedScore());
            result.setMirrored(pixelMatchScore.isMirrored());
            result.setNormalizedScore(pixelMatchScore.getNormalizedScore());
            // set match files
            result.setMatchFileData(FileType.ColorDepthMipInput,
                    maskImage.getNeuronInfo().getNeuronFileData(FileType.ColorDepthMipInput));
            result.setMatchFileData(FileType.ColorDepthMipMatch,
                    targetImage.getNeuronInfo().getNeuronFileData(FileType.ColorDepthMipInput));
            // set compute match files
            result.setMatchComputeFileData(MatchComputeFileType.MaskColorDepthImage,
                    maskImage.getNeuronInfo().getComputeFileData(ComputeFileType.InputColorDepthImage));
            result.setMatchComputeFileData(MatchComputeFileType.MaskGradientImage,
                    maskImage.getNeuronInfo().getComputeFileData(ComputeFileType.GradientImage));
            result.setMatchComputeFileData(MatchComputeFileType.MaskZGapImage,
                    maskImage.getNeuronInfo().getComputeFileData(ComputeFileType.ZGapImage));
        } catch (Throwable e) {
            LOG.warn("Error comparing mask {} with {}", maskImage, targetImage, e);
            result.setErrors(e.getMessage());
        }
        return result;
    }
}
