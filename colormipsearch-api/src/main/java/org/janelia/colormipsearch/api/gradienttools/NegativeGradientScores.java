package org.janelia.colormipsearch.api.gradienttools;

import org.apache.commons.lang3.builder.ToStringBuilder;

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

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("gradientAreaGap", gradientAreaGap)
                .append("highExpressionArea", highExpressionArea)
                .toString();
    }
}
