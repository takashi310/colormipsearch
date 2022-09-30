package org.janelia.colormipsearch.dto;

import javax.validation.constraints.NotNull;

/**
 * Metadata about a color depth matched target.
 *
 * @param <T>
 */
public class CDMatchedTarget<T extends AbstractNeuronMetadata> extends AbstractMatchedTarget<T> {
    private Float normalizedScore;
    private Integer matchingPixels;

    @Override
    public String getTypeDiscriminator() {
        return "CDSMatch";
    }

    @NotNull
    public Float getNormalizedScore() {
        return normalizedScore;
    }

    public void setNormalizedScore(Float normalizedScore) {
        this.normalizedScore = normalizedScore;
    }

    @NotNull
    public Integer getMatchingPixels() {
        return matchingPixels;
    }

    public void setMatchingPixels(Integer matchingPixels) {
        this.matchingPixels = matchingPixels;
    }
}
