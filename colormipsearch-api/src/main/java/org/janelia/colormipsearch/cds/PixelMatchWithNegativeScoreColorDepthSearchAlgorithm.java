package org.janelia.colormipsearch.cds;

import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.janelia.colormipsearch.imageprocessing.ImageArray;
import org.janelia.colormipsearch.model.ComputeFileType;

public class PixelMatchWithNegativeScoreColorDepthSearchAlgorithm implements ColorDepthSearchAlgorithm<ColorMIPMatchScore> {

    private final ColorDepthSearchAlgorithm<ColorMIPMatchScore> cdsMatchScoreCalculator;
    private final ColorDepthSearchAlgorithm<NegativeColorDepthMatchScore> negScoreCDSearchCalculator;

    public PixelMatchWithNegativeScoreColorDepthSearchAlgorithm(ColorDepthSearchAlgorithm<ColorMIPMatchScore> cdsMatchScoreCalculator,
                                                                ColorDepthSearchAlgorithm<NegativeColorDepthMatchScore> negScoreCDSearchCalculator) {
        this.cdsMatchScoreCalculator = cdsMatchScoreCalculator;
        this.negScoreCDSearchCalculator = negScoreCDSearchCalculator;
    }

    @Override
    public ImageArray<?> getQueryImage() {
        return cdsMatchScoreCalculator.getQueryImage();
    }

    @Override
    public int getQuerySize() {
        return cdsMatchScoreCalculator.getQuerySize();
    }

    @Override
    public int getQueryFirstPixelIndex() {
        return cdsMatchScoreCalculator.getQueryFirstPixelIndex();
    }

    @Override
    public int getQueryLastPixelIndex() {
        return cdsMatchScoreCalculator.getQueryLastPixelIndex();
    }

    @Override
    public Set<ComputeFileType> getRequiredTargetVariantTypes() {
        return Stream.concat(
                cdsMatchScoreCalculator.getRequiredTargetVariantTypes().stream(),
                negScoreCDSearchCalculator.getRequiredTargetVariantTypes().stream())
                .collect(Collectors.toSet());
    }

    @Override
    public ColorMIPMatchScore calculateMatchingScore(@Nonnull ImageArray<?> targetImageArray,
                                                     Map<ComputeFileType, Supplier<ImageArray<?>>> variantImageSuppliers) {
        ColorMIPMatchScore cdsMatchScore = cdsMatchScoreCalculator.calculateMatchingScore(targetImageArray, variantImageSuppliers);
        if (cdsMatchScore.getScore() > 0) {
            NegativeColorDepthMatchScore negativeColorDepthMatchScore = negScoreCDSearchCalculator.calculateMatchingScore(targetImageArray, variantImageSuppliers);
            return new ColorMIPMatchScore(cdsMatchScore.getMatchingPixNum(), cdsMatchScore.getMatchingPixNumToMaskRatio(), cdsMatchScore.isMirrored(), negativeColorDepthMatchScore);
        } else {
            return new ColorMIPMatchScore(cdsMatchScore.getMatchingPixNum(), cdsMatchScore.getMatchingPixNumToMaskRatio(), cdsMatchScore.isMirrored(), null);
        }
    }
}
