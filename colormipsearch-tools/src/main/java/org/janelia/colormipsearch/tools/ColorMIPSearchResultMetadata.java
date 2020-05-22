package org.janelia.colormipsearch.tools;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class ColorMIPSearchResultMetadata extends MetadataAttrs {
    public static ColorMIPSearchResultMetadata create(ColorMIPSearchResultMetadata from) {
        ColorMIPSearchResultMetadata cdsCopy = new ColorMIPSearchResultMetadata();
        from.copyTo(cdsCopy);
        cdsCopy.matchedId = from.matchedId;
        return cdsCopy;
    }

    private String imageArchivePath;
    private String imageName;
    private String imageType;
    private String matchedId;
    private String matchedPublishedName;
    private String matchedImageArchivePath;
    private String matchedImageName;
    private String matchedImageType;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    public String getImageArchivePath() {
        return imageArchivePath;
    }

    public void setImageArchivePath(String imageArchivePath) {
        this.imageArchivePath = imageArchivePath;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    public String getImageName() {
        return imageName;
    }

    public void setImageName(String imageName) {
        this.imageName = imageName;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    public String getImageType() {
        return imageType;
    }

    public void setImageType(String imageType) {
        this.imageType = imageType;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    public String getMatchedId() {
        return matchedId;
    }

    public void setMatchedId(String matchedId) {
        this.matchedId = matchedId;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    public String getMatchedPublishedName() {
        return matchedPublishedName;
    }

    public void setMatchedPublishedName(String matchedPublishedName) {
        this.matchedPublishedName = matchedPublishedName;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    public String getMatchedImageArchivePath() {
        return matchedImageArchivePath;
    }

    public void setMatchedImageArchivePath(String matchedImageArchivePath) {
        this.matchedImageArchivePath = matchedImageArchivePath;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    public String getMatchedImageName() {
        return matchedImageName;
    }

    public void setMatchedImageName(String matchedImageName) {
        this.matchedImageName = matchedImageName;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    public String getMatchedImageType() {
        return matchedImageType;
    }

    public void setMatchedImageType(String matchedImageType) {
        this.matchedImageType = matchedImageType;
    }

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
        String gradientAreaGap = StringUtils.defaultIfBlank(
                getAttr("GradientAreaGap"),
                getAttr("Gradient Area Gap"));
        return StringUtils.isBlank(gradientAreaGap) ? -1L : Long.parseLong(gradientAreaGap);
    }

    public void setGradientAreaGap(long gradientAreaGap) {
        if (gradientAreaGap >= 0L) {
            addAttr("GradientAreaGap", String.valueOf(gradientAreaGap));
        } else {
            removeAttr("GradientAreaGap");
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

    @JsonIgnore
    public Double getArtificialShapeScore() {
        String artificialShapeScore = getAttr("ArtificialShapeScore");
        return StringUtils.isBlank(artificialShapeScore) ? null : Double.parseDouble(artificialShapeScore);
    }

    public void setArtificialShapeScore(Double artificialGapScore) {
        if (artificialGapScore != null && artificialGapScore > 0) {
            addAttr("ArtificialShapeScore", artificialGapScore.toString());
        } else {
            removeAttr("ArtificialShapeScore");
        }
    }

    public Double getNormalizedScore() {
        Double normalizedGapScore = getNormalizedGradientAreaGapScore();
        if (normalizedGapScore != null) {
            return normalizedGapScore;
        } else {
            Double artificialGapScore = getArtificialShapeScore();
            return artificialGapScore != null ? artificialGapScore : getMatchingPixels();
        }
    }

    public boolean matches(ColorMIPSearchResultMetadata that) {
        return this.getId() != null && this.getId().equals(that.matchedId) &&
                this.matchedId != null && this.matchedId.equals(that.getId());
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("matchedId", matchedId)
                .append("matchedPublishedName", matchedPublishedName)
                .toString();
    }

    @Override
    String mapAttr(String attrName) {
        if (StringUtils.equalsIgnoreCase(attrName, "Gradient Area Gap")) {
            return "GradientAreaGap";
        } else if (StringUtils.equalsIgnoreCase(attrName, "Genotype")) {
            return null;
        } else {
            return attrName;
        }
    }

}
