package org.janelia.colormipsearch;

import java.io.File;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;

class ColorMIPSearchResultMetadata extends MetadataAttrs {
    static ColorMIPSearchResultMetadata create(ColorMIPSearchResultMetadata from) {
        ColorMIPSearchResultMetadata cdsCopy = new ColorMIPSearchResultMetadata();
        from.copyTo(cdsCopy);
        cdsCopy.matchedId = from.matchedId;
        cdsCopy.matchedImageName = StringUtils.isNotBlank(from.matchedImageName) ? new File(from.matchedImageName).getName() : null;
        return cdsCopy;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    String imageArchivePath;
    @JsonProperty
    String imageName;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    String imageType;
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

    @JsonIgnore
    public int getMatchingPixels() {
        String matchingPixels = getAttr("Matched pixels");
        return StringUtils.isBlank(matchingPixels) ? 0 : Integer.parseInt(matchingPixels);
    }

    public void setMatchingPixels(int matchingPixels) {
        if (matchingPixels > 0) {
            addAttr("Matched pixels", String.valueOf(matchingPixels));
        } else {
            removeAttr("Matched pixels");
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
        } else {
            removeAttr("Score");
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
        } else {
            removeAttr("Gradient Area Gap");
        }
    }

    @JsonIgnore
    public Double getNormalizedGradientAreaGapScore() {
        String normalizedGapScore = getAttr("NormalizedGapScore");
        return StringUtils.isBlank(normalizedGapScore) ? null : Double.parseDouble(normalizedGapScore);
    }

    public void setNormalizedGradientAreaGapScore(Double normalizedGapScore) {
        if (normalizedGapScore != null && normalizedGapScore > 0) {
            addAttr("NormalizedGapScore", normalizedGapScore.toString());
        } else {
            removeAttr("NormalizedGapScore");
        }
    }

    public Double getNormalizedScore() {
        Double normalizedGapScore = getNormalizedGradientAreaGapScore();
        if (normalizedGapScore == null) {
            return normalizedGapScore;
        } else {
            return getMatchingPixelsPct() * 100;
        }
    }

}
