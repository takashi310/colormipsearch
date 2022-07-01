package org.janelia.colormipsearch.cds;

public class PixelMatchScore implements ColorDepthMatchScore {

    private final int matchingPixNum;
    private final double matchingPixNumToMaskRatio;
    private final boolean mirrored;

    public PixelMatchScore(int matchingPixNum,
                           double matchingPixNumToMaskRatio,
                           boolean mirrored) {
        this.matchingPixNum = matchingPixNum;
        this.matchingPixNumToMaskRatio = matchingPixNumToMaskRatio;
        this.mirrored = mirrored;
    }

    @Override
    public int getScore() {
        return matchingPixNum;
    }

    @Override
    public float getNormalizedScore() {
        return (float) matchingPixNumToMaskRatio;
    }

    @Override
    public boolean isMirrored() {
        return mirrored;
    }
}
