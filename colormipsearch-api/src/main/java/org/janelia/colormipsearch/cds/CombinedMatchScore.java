package org.janelia.colormipsearch.cds;

public class CombinedMatchScore {
    private final int pixelMatches;
    private final long gradScore;

    public CombinedMatchScore(int pixelMatchess, long gradScore) {
        this.pixelMatches = pixelMatchess;
        this.gradScore = gradScore;
    }

    public int getPixelMatches() {
        return pixelMatches;
    }

    public long getGradScore() {
        return gradScore;
    }
}
