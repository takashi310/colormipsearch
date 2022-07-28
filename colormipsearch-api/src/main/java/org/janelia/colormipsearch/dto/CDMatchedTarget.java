package org.janelia.colormipsearch.dto;

import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.JsonRequired;

/**
 * Metadata about a color depth matched target.
 *
 * @param <T>
 */
public class CDMatchedTarget<T extends AbstractNeuronEntity> extends AbstractMatchedTarget<T> {
    private Float normalizedScore;
    private Integer matchingPixels;
    private Float matchingPixelsRatio;
    private Long gradientAreaGap;

    @JsonRequired
    public Float getNormalizedScore() {
        return normalizedScore;
    }

    public void setNormalizedScore(Float normalizedScore) {
        this.normalizedScore = normalizedScore;
    }

    @JsonRequired
    public Integer getMatchingPixels() {
        return matchingPixels;
    }

    public void setMatchingPixels(Integer matchingPixels) {
        this.matchingPixels = matchingPixels;
    }

    public Float getMatchingPixelsRatio() {
        return matchingPixelsRatio;
    }

    public void setMatchingPixelsRatio(Float matchingPixelsRatio) {
        this.matchingPixelsRatio = matchingPixelsRatio;
    }

    public Long getGradientAreaGap() {
        return gradientAreaGap;
    }

    public void setGradientAreaGap(Long gradientAreaGap) {
        this.gradientAreaGap = gradientAreaGap;
    }

}
