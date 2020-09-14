package org.janelia.colormipsearch.api.cdsearch;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.janelia.colormipsearch.api.imageprocessing.ImageArray;

public class PixelMatchWithNegativeScoreColorDepthSearchAlgorithm implements ColorDepthSearchAlgorithm<ColorMIPMatchScore> {

    private final ColorDepthSearchAlgorithm<ColorMIPMatchScore> cdsMatchScoreCalculator;
    private final ColorDepthSearchAlgorithm<NegativeColorDepthMatchScore> negScoreCDSearchCalculator;

    public PixelMatchWithNegativeScoreColorDepthSearchAlgorithm(ColorDepthSearchAlgorithm<ColorMIPMatchScore> cdsMatchScoreCalculator,
                                                                ColorDepthSearchAlgorithm<NegativeColorDepthMatchScore> negScoreCDSearchCalculator) {
        this.cdsMatchScoreCalculator = cdsMatchScoreCalculator;
        this.negScoreCDSearchCalculator = negScoreCDSearchCalculator;
    }

    @Override
    public ImageArray getQueryImage() {
        return cdsMatchScoreCalculator.getQueryImage();
    }

    @Override
    public ColorMIPMatchScore calculateMatchingScore(@Nonnull ImageArray targetImageArray, @Nullable ImageArray targetGradientImageArray, @Nullable ImageArray targetZGapMaskImageArray) {
        ColorMIPMatchScore cdsMatchScore = cdsMatchScoreCalculator.calculateMatchingScore(targetImageArray, targetGradientImageArray, targetZGapMaskImageArray);
        if (cdsMatchScore.getScore() > 0) {
            NegativeColorDepthMatchScore negativeColorDepthMatchScore = negScoreCDSearchCalculator.calculateMatchingScore(targetImageArray, targetGradientImageArray, targetZGapMaskImageArray);
            return new ColorMIPMatchScore(cdsMatchScore.getMatchingPixNum(), cdsMatchScore.getMatchingPixNumToMaskRatio(), negativeColorDepthMatchScore);
        } else {
            return new ColorMIPMatchScore(cdsMatchScore.getMatchingPixNum(), cdsMatchScore.getMatchingPixNumToMaskRatio(), null);
        }
    }
}
