package org.janelia.colormipsearch.api.gradienttools;

public class NegativeGradientScores {
    public final long gradientAreaGap;
    public final long highExpressionArea;

    NegativeGradientScores(long gradientAreaGap, long highExpressionArea) {
        this.gradientAreaGap = gradientAreaGap;
        this.highExpressionArea = highExpressionArea;
    }

    public long getCumulatedScore() {
        return gradientAreaGap + highExpressionArea / 3;
    }
}
