package org.janelia.colormipsearch.api_v2.cdsearch;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import org.janelia.colormipsearch.imageprocessing.ImageArray;

@Deprecated
public class PixelMatchWithNegativeScoreColorDepthSearchAlgorithm implements ColorDepthSearchAlgorithm<ColorMIPMatchScore> {

    private final Set<String> requiredVariantTypes = new LinkedHashSet<>();
    private final ColorDepthSearchAlgorithm<ColorMIPMatchScore> cdsMatchScoreCalculator;
    private final ColorDepthSearchAlgorithm<NegativeColorDepthMatchScore> negScoreCDSearchCalculator;

    public PixelMatchWithNegativeScoreColorDepthSearchAlgorithm(ColorDepthSearchAlgorithm<ColorMIPMatchScore> cdsMatchScoreCalculator,
                                                                ColorDepthSearchAlgorithm<NegativeColorDepthMatchScore> negScoreCDSearchCalculator) {
        this.cdsMatchScoreCalculator = cdsMatchScoreCalculator;
        this.negScoreCDSearchCalculator = negScoreCDSearchCalculator;
        requiredVariantTypes.addAll(cdsMatchScoreCalculator.getRequiredTargetVariantTypes());
        requiredVariantTypes.addAll(negScoreCDSearchCalculator.getRequiredTargetVariantTypes());
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
    public Set<String> getRequiredTargetVariantTypes() {
        return requiredVariantTypes;
    }

    @Override
    public ColorMIPMatchScore calculateMatchingScore(@Nonnull ImageArray<?> targetImageArray,
                                                     Map<String, Supplier<ImageArray<?>>> variantTypeSuppliers) {
        ColorMIPMatchScore cdsMatchScore = cdsMatchScoreCalculator.calculateMatchingScore(targetImageArray, variantTypeSuppliers);
        if (cdsMatchScore.getScore() > 0) {
            NegativeColorDepthMatchScore negativeColorDepthMatchScore = negScoreCDSearchCalculator.calculateMatchingScore(targetImageArray, variantTypeSuppliers);
            return new ColorMIPMatchScore(cdsMatchScore.getMatchingPixNum(), cdsMatchScore.getMatchingPixNumToMaskRatio(), cdsMatchScore.isMirrored(), negativeColorDepthMatchScore);
        } else {
            return new ColorMIPMatchScore(cdsMatchScore.getMatchingPixNum(), cdsMatchScore.getMatchingPixNumToMaskRatio(), cdsMatchScore.isMirrored(), null);
        }
    }
}
