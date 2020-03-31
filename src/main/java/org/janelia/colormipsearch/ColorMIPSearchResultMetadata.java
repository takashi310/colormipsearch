package org.janelia.colormipsearch;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;

class ColorMIPSearchResultMetadata extends MetadataAttrs {
    @JsonProperty
    String matchedId;
    @JsonProperty
    String imageArchivePath;
    @JsonProperty
    String imageName;
    @JsonProperty
    String imageType;
    @JsonProperty
    int imageVolumeSize;
    @JsonProperty
    double imageShapeScore;
    @JsonProperty
    String matchedImageArchivePath;
    @JsonProperty
    String matchedImageName;
    @JsonProperty
    String matchedImageType;
    @JsonProperty
    int matchedImageVolumeSize;
    @JsonProperty
    double matchedImageShapeScore;

    @JsonIgnore
    public int getMatchingPixels() {
        String matchingPixels = getAttr("Matched slices");
        return StringUtils.isBlank(matchingPixels) ? 0 : Integer.parseInt(matchingPixels);
    }

    public void setMatchingPixels(int matchingPixels) {
        if (matchingPixels > 0) {
            addAttr("Matched slices", String.valueOf(matchingPixels));
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
}
