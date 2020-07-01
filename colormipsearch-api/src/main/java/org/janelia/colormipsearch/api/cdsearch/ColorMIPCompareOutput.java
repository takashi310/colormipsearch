package org.janelia.colormipsearch.api.cdsearch;

/**
 * ColorMIPCompareOutput represents the color depth match summary result.
 */
public class ColorMIPCompareOutput {
    private final int matchingPixNum;
    private final double matchingPixNumToMaskRatio;

    public static final ColorMIPCompareOutput NO_MATCH = new ColorMIPCompareOutput(0, 0);

    ColorMIPCompareOutput(int pixnum, double pct) {
        matchingPixNum = pixnum;
        matchingPixNumToMaskRatio = pct;
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
}
