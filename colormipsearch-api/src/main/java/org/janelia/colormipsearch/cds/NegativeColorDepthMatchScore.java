package org.janelia.colormipsearch.cds;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class NegativeColorDepthMatchScore implements ColorDepthMatchScore {
    private final long gradientAreaGap;
    private final long highExpressionArea;
    private final boolean mirrored;

    NegativeColorDepthMatchScore(long gradientAreaGap, long highExpressionArea, boolean mirrored) {
        this.gradientAreaGap = gradientAreaGap;
        this.highExpressionArea = highExpressionArea;
        this.mirrored = mirrored;
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
    public boolean isMirrored() {
        return mirrored;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("gradientAreaGap", gradientAreaGap)
                .append("highExpressionArea", highExpressionArea)
                .toString();
    }
}
