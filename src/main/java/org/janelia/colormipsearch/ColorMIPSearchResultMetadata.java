package org.janelia.colormipsearch;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;

class ColorMIPSearchResultMetadata extends MetadataAttrs {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    String imageArchivePath;
    @JsonProperty
    String imageName;
    @JsonProperty
    String imageType;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    Integer imageVolumeSize;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    Double imageShapeScore;
    @JsonProperty
    String matchedId;
    @JsonProperty
    String matchedPublishedName;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    String matchedImageArchivePath;
    @JsonProperty
    String matchedImageName;
    @JsonProperty
    String matchedImageType;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    Integer matchedImageVolumeSize;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    Double matchedImageShapeScore;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    Long maxGradientAreaGap;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    Double normGradientAreaGap;

    @JsonIgnore
    public int getMatchingPixels() {
        String matchingPixels = getAttr("Matched pixels");
        return StringUtils.isBlank(matchingPixels) ? 0 : Integer.parseInt(matchingPixels);
    }

    public void setMatchingPixels(int matchingPixels) {
        if (matchingPixels > 0) {
            addAttr("Matched pixels", String.valueOf(matchingPixels));
        }
    }

    @JsonIgnore
    public double getMatchingPixelsPct() {
        String matchingPixelsPct = getAttr("Score");
        return StringUtils.isBlank(matchingPixelsPct) ? 0. : Double.parseDouble(matchingPixelsPct);
    }

    public void setMatchingPixelsPct(double matchingPixelsPct) {
        if (matchingPixelsPct > 0.) {
            addAttr("Score", String.valueOf(matchingPixelsPct));
        }
    }

    @JsonIgnore
    public long getGradientAreaGap() {
        String gradientAreaGap = getAttr("Gradient Area Gap");
        return StringUtils.isBlank(gradientAreaGap) ? -1L : Long.parseLong(gradientAreaGap);
    }

    public void setGradientAreaGap(long gradientAreaGap) {
        if (gradientAreaGap >= 0L) {
            addAttr("Gradient Area Gap", String.valueOf(gradientAreaGap));
        }
    }

    public Double getNormalizedGapScore() {
        String normalizedGapScore = getAttr("NormalizedGapScore");
        return StringUtils.isBlank(normalizedGapScore) ? null : Double.parseDouble(normalizedGapScore);
    }

    public void setNormalizedGapScore(Double normalizedGapScore) {
        if (normalizedGapScore == null) {
            attrs.remove("NormalizedGapScore");
        } else {
            attrs.put("NormalizedGapScore", normalizedGapScore.toString());
        }
    }

}
