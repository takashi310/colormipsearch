package org.janelia.colormipsearch.model;

public class CDSMatch<I extends AbstractNeuronImage> extends AbstractMatch<I> {
    private Float normalizedScore;
    private Integer matchingPixels;

    public Float getNormalizedScore() {
        return normalizedScore;
    }

    public void setNormalizedScore(Float normalizedScore) {
        this.normalizedScore = normalizedScore;
    }

    public Integer getMatchingPixels() {
        return matchingPixels;
    }

    public void setMatchingPixels(Integer matchingPixels) {
        this.matchingPixels = matchingPixels;
    }
}
