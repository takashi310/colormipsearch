package org.janelia.colormipsearch.cmd.cdsprocess;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.cds.ColorDepthPixelMatchScore;
import org.janelia.colormipsearch.cds.ColorDepthSearchAlgorithm;
import org.janelia.colormipsearch.cds.ColorMIPSearch;
import org.janelia.colormipsearch.cmd.CachedMIPsUtils;
import org.janelia.colormipsearch.imageprocessing.ImageArray;
import org.janelia.colormipsearch.mips.NeuronMIP;
import org.janelia.colormipsearch.mips.NeuronMIPUtils;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDSMatch;
import org.janelia.colormipsearch.model.ComputeFileType;
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
        return variantTypes.stream()
                .map(cft -> {
                    Pair<ComputeFileType, Supplier<ImageArray<?>>> e =
                            ImmutablePair.of(
                                    cft,
                                    () -> NeuronMIPUtils.getImageArray(CachedMIPsUtils.loadMIP(neuronMIP, cft))
                            );
                    return e;
                })
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight))
                ;
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
    CDSMatch<M, T> findPixelMatch(ColorDepthSearchAlgorithm<ColorDepthPixelMatchScore> cdsAlgorithm,
                                  NeuronMIP<M> maskImage,
                                  NeuronMIP<T> targetImage) {
        CDSMatch<M, T> result = new CDSMatch<>();
        result.setMaskImage(maskImage.getNeuronInfo());
        result.setMatchedImage(targetImage.getNeuronInfo());
        try {
            Map<ComputeFileType, Supplier<ImageArray<?>>> variantImageSuppliers =
                    getVariantImagesSuppliers(cdsAlgorithm.getRequiredTargetVariantTypes(), targetImage.getNeuronInfo());
            ColorDepthPixelMatchScore pixelMatchScore = cdsAlgorithm.calculateMatchingScore(
                    NeuronMIPUtils.getImageArray(targetImage),
                    variantImageSuppliers);
            result.setMatchFound(colorMIPSearch.isMatch(pixelMatchScore));
            result.setMatchingPixels(pixelMatchScore.getScore());
            result.setMirrored(pixelMatchScore.isMirrored());
            result.setNormalizedScore(pixelMatchScore.getNormalizedScore());
        } catch (Throwable e) {
            LOG.warn("Error comparing mask {} with {}", maskImage, targetImage, e);
            result.setErrors(e.getMessage());
        }
        return result;
    }
}
