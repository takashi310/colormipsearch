package org.janelia.colormipsearch.api;

/**
 * ColorMIPCompareOutput represents the color depth match summary result.
 */
public class ColorMIPCompareOutput {
    private final int matchingPixNum;
    private final double matchingPct;

    public ColorMIPCompareOutput(int pixnum, double pct) {
        matchingPixNum = pixnum;
        matchingPct = pct;
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
    public double getMatchingPct() {
        return matchingPct;
    }
}
