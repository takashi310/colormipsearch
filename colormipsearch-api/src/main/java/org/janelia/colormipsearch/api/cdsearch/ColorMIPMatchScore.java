package org.janelia.colormipsearch.api.cdsearch;

import java.io.Serializable;

import javax.annotation.Nullable;

/**
 * ColorMIPCompareOutput represents the color depth match summary result.
 */
public class ColorMIPMatchScore implements ColorDepthMatchScore {
    public static ColorMIPMatchScore NO_MATCH = new ColorMIPMatchScore(0, 0, null);

    private final int matchingPixNum;
    private final double matchingPixNumToMaskRatio;
    private final NegativeColorDepthMatchScore negativeScores;

    public ColorMIPMatchScore(int matchingPixNum,
                       double matchingPixNumToMaskRatio,
                       @Nullable NegativeColorDepthMatchScore negativeScores) {
        this.matchingPixNum = matchingPixNum;
        this.matchingPixNumToMaskRatio = matchingPixNumToMaskRatio;
        this.negativeScores = negativeScores;
    }

    @Override
    public long getScore() {
        return matchingPixNum;
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
