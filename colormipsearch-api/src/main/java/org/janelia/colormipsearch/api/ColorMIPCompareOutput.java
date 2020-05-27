package org.janelia.colormipsearch.api;

public class ColorMIPCompareOutput {
    private final int matchingPixNum;
    private final double matchingPct;

    ColorMIPCompareOutput(int pixnum, double pct) {
        matchingPixNum = pixnum;
        matchingPct = pct;
    }

    public int getMatchingPixNum() {
        return matchingPixNum;
    }

    public double getMatchingPct() {
        return matchingPct;
    }
}
