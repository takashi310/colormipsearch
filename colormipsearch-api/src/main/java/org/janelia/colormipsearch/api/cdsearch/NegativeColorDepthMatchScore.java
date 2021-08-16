package org.janelia.colormipsearch.api.cdsearch;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class NegativeColorDepthMatchScore implements ColorDepthMatchScore {
    private final long gradientAreaGap;
    private final long highExpressionArea;
    private final boolean bestScoreMirrored;

    NegativeColorDepthMatchScore(long gradientAreaGap, long highExpressionArea, boolean bestScoreMirrored) {
        this.gradientAreaGap = gradientAreaGap;
        this.highExpressionArea = highExpressionArea;
        this.bestScoreMirrored = bestScoreMirrored;
    }

    public long getGradientAreaGap() {
        return gradientAreaGap;
    }

    public long getHighExpressionArea() {
        return highExpressionArea;
    }

    @Override
    public long getScore() {
        return GradientAreaGapUtils.calculateNegativeScore(gradientAreaGap, highExpressionArea);
    }

    @Override
    public boolean isBestScoreMirrored() {
        return false;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("gradientAreaGap", gradientAreaGap)
                .append("highExpressionArea", highExpressionArea)
                .toString();
    }
}
