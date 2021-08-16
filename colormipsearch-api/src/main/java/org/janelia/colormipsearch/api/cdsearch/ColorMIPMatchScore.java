package org.janelia.colormipsearch.api.cdsearch;

import javax.annotation.Nullable;

/**
 * ColorMIPCompareOutput represents the color depth match summary result.
 */
public class ColorMIPMatchScore implements ColorDepthMatchScore {
    public static ColorMIPMatchScore NO_MATCH = new ColorMIPMatchScore(0, 0, false, null);

    private final int matchingPixNum;
    private final double matchingPixNumToMaskRatio;
    private final boolean bestMatchMirrored;
    private final NegativeColorDepthMatchScore negativeScores;

    public ColorMIPMatchScore(int matchingPixNum,
                       double matchingPixNumToMaskRatio,
                       boolean bestMatchMirrored,
                       @Nullable NegativeColorDepthMatchScore negativeScores) {
        this.matchingPixNum = matchingPixNum;
        this.matchingPixNumToMaskRatio = matchingPixNumToMaskRatio;
        this.bestMatchMirrored = bestMatchMirrored;
        this.negativeScores = negativeScores;
    }

    @Override
    public long getScore() {
        return matchingPixNum;
    }

    @Override
    public boolean isBestScoreMirrored() {
        return bestMatchMirrored;
    }

    public boolean isMatch() {
        return matchingPixNum > 0;
    }

    /**
     * @return the number of matching pixels
     */
    public int getMatchingPixNum() {
        return matchingPixNum;
    }

    /**
     * @return the ratio of the matching pixels to the size of the mask.
     */
    public double getMatchingPixNumToMaskRatio() {
        return matchingPixNumToMaskRatio;
    }

    public long getGradientAreaGap() {
        return negativeScores != null ? negativeScores.getGradientAreaGap() : -1;
    }

    public long getHighExpressionArea() {
        return negativeScores != null ? negativeScores.getHighExpressionArea() : -1;
    }

}
