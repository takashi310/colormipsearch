package org.janelia.colormipsearch.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.cds.GradientAreaGapUtils;
import org.janelia.colormipsearch.dto.AbstractNeuronMetadata;
import org.janelia.colormipsearch.dto.CDMatchedTarget;
import org.janelia.colormipsearch.model.annotations.PersistenceInfo;

@PersistenceInfo(storeName ="cdMatches")
public class CDMatchEntity<M extends AbstractNeuronEntity, T extends AbstractNeuronEntity> extends AbstractMatchEntity<M, T> {
    private Float normalizedScore;
    private Integer matchingPixels;
    private Float matchingPixelsRatio;
    private Long gradientAreaGap;
    private Long highExpressionArea;
    private boolean matchFound;
    private String errors;

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

    public Long getHighExpressionArea() {
        return highExpressionArea;
    }

    public void setHighExpressionArea(Long highExpressionArea) {
        this.highExpressionArea = highExpressionArea;
    }

    @JsonIgnore
    public Long getGradScore() {
        return hasGradScore()
                ? GradientAreaGapUtils.calculateNegativeScore(gradientAreaGap, highExpressionArea)
                : -1;
    }

    public boolean hasGradScore() {
        return gradientAreaGap != null && gradientAreaGap >= 0 && highExpressionArea != null && highExpressionArea > 0;
    }

    @JsonIgnore
    public boolean isMatchFound() {
        return matchFound;
    }

    public void setMatchFound(boolean matchFound) {
        this.matchFound = matchFound;
    }

    @JsonIgnore
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
        return !hasErrors();
    }

    @SuppressWarnings("unchecked")
    @Override
    public CDMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity> duplicate(
            MatchCopier<AbstractMatchEntity<AbstractNeuronEntity, AbstractNeuronEntity>, AbstractMatchEntity<AbstractNeuronEntity, AbstractNeuronEntity>> copier) {
        CDMatchEntity<AbstractNeuronEntity, AbstractNeuronEntity> clone = new CDMatchEntity<>();
        // copy fields that are safe to copy
        clone.safeFieldsCopyFrom(this);
        // copy fields specific to this class
        clone.normalizedScore = this.normalizedScore;
        clone.matchingPixels = this.matchingPixels;
        clone.matchingPixelsRatio = this.matchingPixelsRatio;
        clone.gradientAreaGap = this.gradientAreaGap;
        clone.highExpressionArea = this.highExpressionArea;
        clone.matchFound = this.matchFound;
        clone.errors = this.errors;
        // apply the copier
        copier.copy((AbstractMatchEntity<AbstractNeuronEntity, AbstractNeuronEntity>) this, clone);
        return clone;
    }

    @Override
    public CDMatchedTarget<? extends AbstractNeuronMetadata> metadata() {
        CDMatchedTarget<AbstractNeuronMetadata> m = new CDMatchedTarget<>();
        AbstractNeuronMetadata n = getMatchedImage().metadata();
        m.setTargetImage(n);
        m.setMirrored(isMirrored());
        m.setNormalizedScore(getNormalizedScore());
        m.setMatchingPixels(getMatchingPixels());
        m.setMatchingPixelsRatio(getMatchingPixelsRatio());
        m.setGradientAreaGap(getGradientAreaGap());
        m.setMatchFiles(getMatchFiles());
        return m;
    }
}
