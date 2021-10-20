package org.janelia.colormipsearch.cmd;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchMatchMetadata;

public class LightColorMIPSearchMatch {

    private String sourceId;
    private String sourceImageName;
    private String id;
    private String imageName;
    private int matchingPixels;
    private double matchingRatio;
    private Long gradientAreaGap;
    private Long highExpressionArea;
    private Double normalizedGapScore;

    /**
     * This is field will not written for every match in a result file because it only adds a lot of noise.
     *
     * @return
     */
    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getSourceImageName() {
        return sourceImageName;
    }

    public void setSourceImageName(String sourceImageName) {
        this.sourceImageName = sourceImageName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getImageName() {
        return imageName;
    }

    public void setImageName(String imageName) {
        this.imageName = imageName;
    }

    public int getMatchingPixels() {
        return matchingPixels;
    }

    public void setMatchingPixels(int matchingPixels) {
        this.matchingPixels = Math.max(0, matchingPixels);
    }

    public double getMatchingRatio() {
        return matchingRatio;
    }

    public void setMatchingRatio(double matchingRatio) {
        this.matchingRatio = matchingRatio;
    }

    public Long getGradientAreaGap() {
        return gradientAreaGap;
    }

    public void setGradientAreaGap(Long gradientAreaGap) {
        this.gradientAreaGap = gradientAreaGap == null ? -1 : gradientAreaGap;
    }

    public Long getHighExpressionArea() {
        return highExpressionArea;
    }

    public void setHighExpressionArea(Long highExpressionArea) {
        this.highExpressionArea = highExpressionArea == null ? -1 : highExpressionArea;
    }

    public Double getNormalizedGapScore() {
        return normalizedGapScore;
    }

    public void setNormalizedGapScore(Double normalizedGapScore) {
        this.normalizedGapScore = normalizedGapScore;
    }

    boolean hasNegativeScore() {
        return gradientAreaGap != null && gradientAreaGap >= 0;
    }

    boolean exactMatch(ColorMIPSearchMatchMetadata that) {
        return mipMatch(that) &&
                this.getImageName() != null && this.getImageName().equals(that.getSourceImageName()) &&
                this.getSourceImageName() != null && this.getSourceImageName().equals(that.getImageName());
    }

    boolean mipMatch(ColorMIPSearchMatchMetadata that) {
        return this.getId() != null && this.getId().equals(that.getSourceId()) &&
                this.getSourceId() != null && this.getSourceId().equals(that.getId());
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("sourceId", sourceId)
                .append("sourceImageName", sourceImageName)
                .append("id", id)
                .append("imageName", imageName)
                .toString();
    }

}
