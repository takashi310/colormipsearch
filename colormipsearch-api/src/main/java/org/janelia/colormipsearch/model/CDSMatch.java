package org.janelia.colormipsearch.model;

public class CDSMatch<M extends AbstractNeuronMetadata, I extends AbstractNeuronMetadata> extends AbstractMatch<M, I> {
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
