package org.janelia.colormipsearch.model;

import org.apache.commons.lang3.StringUtils;

public class CDSMatch<M extends AbstractNeuronMetadata, I extends AbstractNeuronMetadata> extends AbstractMatch<M, I> {
    private Float normalizedScore;
    private Integer matchingPixels;
    private String errors;

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

    public String getErrors() {
        return errors;
    }

    public void setErrors(String errors) {
        this.errors = errors;
    }

    public boolean hasErrors() {
        return StringUtils.isNotBlank(errors);
    }

    public boolean hasNoErrors() {
        return StringUtils.isBlank(errors);
    }

    @Override
    public <M2 extends AbstractNeuronMetadata, T2 extends AbstractNeuronMetadata> CDSMatch<M2, T2> duplicate(MatchCopier<M, I, AbstractMatch<M, I>, M2, T2, AbstractMatch<M2, T2>> copier) {
        CDSMatch<M2, T2> clone = new CDSMatch<>();
        clone.copyFrom(this);
        copier.copy(this, clone);
        return clone;
    }

}
