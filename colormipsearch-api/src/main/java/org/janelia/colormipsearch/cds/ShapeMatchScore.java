package org.janelia.colormipsearch.cds;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class ShapeMatchScore implements ColorDepthMatchScore {
    private final long gradientAreaGap;
    private final long highExpressionArea;
    private final long maxGradientAreaGap;
    private final boolean mirrored;

    ShapeMatchScore(long gradientAreaGap, long highExpressionArea, long maxGradientAreaGap, boolean mirrored) {
        this.gradientAreaGap = gradientAreaGap;
        this.highExpressionArea = highExpressionArea;
        this.maxGradientAreaGap = maxGradientAreaGap;
        this.mirrored = mirrored;
    }

    @Override
    public int getScore() {
        return (int) GradientAreaGapUtils.calculateNegativeScore(gradientAreaGap, highExpressionArea);
    }

    @Override
    public float getNormalizedScore() {
        long currentScore = getScore();
        return maxGradientAreaGap > 0 ? currentScore / (float) maxGradientAreaGap : currentScore;
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
