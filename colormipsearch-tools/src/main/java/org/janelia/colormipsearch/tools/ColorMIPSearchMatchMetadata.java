package org.janelia.colormipsearch.tools;

import java.util.function.Consumer;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class ColorMIPSearchMatchMetadata extends MIPMetadata {

    public static ColorMIPSearchMatchMetadata create(ColorMIPSearchMatchMetadata from) {
        ColorMIPSearchMatchMetadata cdsCopy = new ColorMIPSearchMatchMetadata();
        from.copyTo(cdsCopy);
        cdsCopy.sourceId = from.sourceId;
        cdsCopy.sourcePublishedName = from.sourcePublishedName;
        cdsCopy.sourceLibraryName = from.sourceLibraryName;
        return cdsCopy;
    }

    private String sourceId;
    private String sourcePublishedName;
    private String sourceLibraryName;
    private String sourceImageName;
    private String sourceImageArchivePath;
    private String sourceImageType;
    private int matchingPixels;
    private double matchingRatio;
    @JsonProperty
    private Long gradientAreaGap;
    private Double normalizedGapScore;
    private Double artificialShapeScore;

    public String getSourceImageArchivePath() {
        return sourceImageArchivePath;
    }

    public void setSourceImageArchivePath(String sourceImageArchivePath) {
        this.sourceImageArchivePath = sourceImageArchivePath;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getSourcePublishedName() {
        return sourcePublishedName;
    }

    public void setSourcePublishedName(String sourcePublishedName) {
        this.sourcePublishedName = sourcePublishedName;
    }

    public String getSourceLibraryName() {
        return sourceLibraryName;
    }

    public void setSourceLibraryName(String sourceLibraryName) {
        this.sourceLibraryName = sourceLibraryName;
    }

    public String getSourceImageName() {
        return sourceImageName;
    }

    public void setSourceImageName(String sourceImageName) {
        this.sourceImageName = sourceImageName;
    }

    public String getSourceImageType() {
        return sourceImageType;
    }

    public void setSourceImageType(String sourceImageType) {
        this.sourceImageType = sourceImageType;
    }

    public int getMatchingPixels() {
        return matchingPixels;
    }

    public void setMatchingPixels(int matchingPixels) {
        this.matchingPixels = Math.max(0, matchingPixels);
    }

    private void updateMatchingPixels(String matchingPixelsValue) {
        if (StringUtils.isBlank(matchingPixelsValue))
            setMatchingPixels(0);
        else
            setMatchingPixels(Integer.parseInt(matchingPixelsValue));
    }

    public double getMatchingRatio() {
        return matchingRatio;
    }

    public void setMatchingRatio(double matchingRatio) {
        this.matchingRatio = matchingRatio;
    }

    private void updateMatchingRatio(String matchingRatioValue) {
        if (StringUtils.isBlank(matchingRatioValue))
            setMatchingRatio(0.);
        else
            setMatchingRatio(Double.parseDouble(matchingRatioValue));
    }

    @JsonIgnore
    public long getGradientAreaGap() {
        return gradientAreaGap == null ? -1 : gradientAreaGap;
    }

    public void setGradientAreaGap(long gradientAreaGap) {
        this.gradientAreaGap = gradientAreaGap >= 0 ? gradientAreaGap : null;
    }

    public Double getNormalizedGapScore() {
        return normalizedGapScore;
    }

    public void setNormalizedGapScore(Double normalizedGapScore) {
        this.normalizedGapScore = normalizedGapScore;
    }

    private void updateNormalizedGapScore(String normalizedGapScoreValue) {
        if (StringUtils.isBlank(normalizedGapScoreValue))
            setNormalizedGapScore(null);
        else
            setNormalizedGapScore(Double.parseDouble(normalizedGapScoreValue));
    }

    private void updateGradientAreaGap(String gradientAreaGapValue) {
        if (StringUtils.isBlank(gradientAreaGapValue))
            setGradientAreaGap(-1L);
        else
            setGradientAreaGap(Long.parseLong(gradientAreaGapValue));
    }

    public Double getArtificialShapeScore() {
        return artificialShapeScore;
    }

    public void setArtificialShapeScore(Double artificialShapeScore) {
        this.artificialShapeScore = artificialShapeScore;
    }

    private void updateArtificialShapeScore(String artificialShapeScoreValue) {
        if (StringUtils.isBlank(artificialShapeScoreValue))
            setArtificialShapeScore(null);
        else
            setArtificialShapeScore(Double.parseDouble(artificialShapeScoreValue));
    }

    public Double getNormalizedScore() {
        if (normalizedGapScore != null) {
            return normalizedGapScore;
        } else {
            return artificialShapeScore != null ? artificialShapeScore : getMatchingPixels();
        }
    }

    public boolean matches(ColorMIPSearchMatchMetadata that) {
        return this.getSourceId() != null && this.getSourceId().equals(that.getId()) &&
                this.getId() != null && this.getId().equals(that.getSourceId());
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("sourceId", sourceId)
                .append("sourcePublishedName", sourcePublishedName)
                .append("sourceLibraryName", sourceLibraryName)
                .append("sourceImageName", sourceImageName)
                .appendSuper(super.toString())
                .toString();
    }

    Consumer<String> attributeValueHandler(String attrName) {
        if (StringUtils.isBlank(attrName)) {
            return (attrValue) -> {}; // do nothing handler
        } else {
            switch (attrName) {
                case "matchedId":
                    return this::setId;
                case "matchedPublishedName":
                    return this::setPublishedName;
                case "matchedLibrary":
                    return this::setLibraryName;
                case "matchedImageName":
                    return this::setImageName;
                case "matchedImageArchivePath":
                    return this::setImageArchivePath;
                case "matchedImageType":
                    return this::setImageType;
                case "gradientAreaGap":
                    return this::updateGradientAreaGap;
                case "matchingPixels":
                    return this::updateMatchingPixels;
                case "matchingRatio":
                    return this::updateMatchingRatio;
                case "normalizedGapScore":
                    return this::updateNormalizedGapScore;
                case "artificialShapeScore":
                    return this::updateArtificialShapeScore;
                default:
                    return defaultAttributeValueHandler(attrName);
            }
        }
    }

    @Override
    String mapAttr(String attrName) {
        if (StringUtils.isBlank(attrName)) {
            return null;
        }
        switch(attrName.toLowerCase()) {
            case "published name":
            case "publishedname":
                return "matchedPublishedName";
            case "library":
                return "matchedLibrary";
            case "gradient area gap":
            case "gradientareagap":
                return "gradientAreaGap";
            case "matched pixels":
            case "matchingpixels":
                return "matchingPixels";
            case "matchingpixelspct":
            case "score":
            case "matchingratio":
                return "matchingRatio";
            case "normalizedgapscore":
                return "normalizedGapScore";
            case "artificialshapescore":
                return "artificialShapeScore";
        }
        return super.mapAttr(attrName);
    }

}
