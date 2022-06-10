package org.janelia.colormipsearch.api.cdsearch;

import java.nio.file.Paths;
import java.util.function.Consumer;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.api.JsonRequired;
import org.janelia.colormipsearch.api.cdmips.AbstractMetadata;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;

@JsonClassDescription("Color Depth Search Match")
public class ColorMIPSearchMatchMetadata extends AbstractMetadata {

    /**
     * Creates a copy for the release that removes all internal attributes.
     * @param from
     * @return
     */
    public static ColorMIPSearchMatchMetadata createReleaseCopy(ColorMIPSearchMatchMetadata from) {
        ColorMIPSearchMatchMetadata cdsCopy = new ColorMIPSearchMatchMetadata();
        from.copyTo(cdsCopy);
        // reset source image paths
        cdsCopy.setSourceCdmPath(null);
        cdsCopy.setSourceImageType(null);
        cdsCopy.setSourceImageName(null);
        cdsCopy.setSourceImageArchivePath(null);
        // reset image paths
        cdsCopy.setCdmPath(null);
        cdsCopy.setImageType(null);
        cdsCopy.setImageName(null);
        cdsCopy.setImageArchivePath(null);
        // reset other internal fields
        cdsCopy.setSampleRef(null);
        return cdsCopy;
    }

    public static MIPMetadata getQueryMIP(ColorMIPSearchMatchMetadata cdsm) {
        MIPMetadata mip = new MIPMetadata();
        mip.setId(cdsm.getSourceId());
        mip.setPublishedName(cdsm.getSourcePublishedName());
        mip.setCdmPath(cdsm.getSourceCdmPath());
        mip.setImageArchivePath(cdsm.getSourceImageArchivePath());
        mip.setImageName(cdsm.getSourceImageName());
        mip.setImageType(cdsm.getSourceImageType());
        mip.setImageURL(cdsm.getSourceImageURL());
        mip.setSearchablePNG(cdsm.getSourceSearchablePNG());
        mip.setImageStack(cdsm.getSourceImageStack());
        mip.setScreenImage(cdsm.getSourceScreenImage());
        mip.copyVariantsFrom(cdsm);
        return mip;
    }

    public static MIPMetadata getTargetMIP(ColorMIPSearchMatchMetadata cdsm) {
        MIPMetadata mip = new MIPMetadata();
        mip.setId(cdsm.getId());
        mip.setPublishedName(cdsm.getPublishedName());
        mip.setCdmPath(cdsm.getCdmPath());
        mip.setImageArchivePath(cdsm.getImageArchivePath());
        mip.setImageName(cdsm.getImageName());
        mip.setImageType(cdsm.getImageType());
        mip.setImageURL(cdsm.getImageURL());
        mip.setSearchablePNG(cdsm.getSearchablePNG());
        mip.setImageStack(cdsm.getImageStack());
        mip.setScreenImage(cdsm.getScreenImage());
        mip.copyVariantsFrom(cdsm);
        return mip;
    }

    private String sourceId;
    private String sourcePublishedName;
    private String sourceLibraryName;
    private String sourceCdmPath;
    private String sourceImageName;
    private String sourceImageArchivePath;
    private String sourceImageType;
    private String sourceSampleRef;
    private String sourceRelatedImageRefId;
    private String sourceImageURL;
    private String sourceSearchablePNG;
    private String sourceImageStack;
    private String sourceScreenImage;
    private int matchingPixels;
    private double matchingRatio;
    private boolean mirrored;
    @JsonProperty
    private Long gradientAreaGap;
    @JsonProperty
    private Long highExpressionArea;
    @JsonRequired
    private Double normalizedGapScore;
    private Double artificialShapeScore;

    /**
     * This is field will not written for every match in a result file because it only adds a lot of noise.
     *
     * @return
     */
    @JsonIgnore
    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    /**
     * This is field will not written for every match in a result file because it only adds a lot of noise.
     *
     * @return
     */
    @JsonIgnore
    public String getSourcePublishedName() {
        return sourcePublishedName;
    }

    public void setSourcePublishedName(String sourcePublishedName) {
        this.sourcePublishedName = sourcePublishedName;
    }

    /**
     * This is field will not written for every match in a result file because it only adds a lot of noise.
     *
     * @return
     */
    @JsonIgnore
    public String getSourceLibraryName() {
        return sourceLibraryName;
    }

    public void setSourceLibraryName(String sourceLibraryName) {
        this.sourceLibraryName = sourceLibraryName;
    }

    public String getSourceCdmPath() {
        return sourceCdmPath;
    }

    public void setSourceCdmPath(String sourceCdmPath) {
        this.sourceCdmPath = sourceCdmPath;
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

    public String getSourceImageArchivePath() {
        return sourceImageArchivePath;
    }

    public void setSourceImageArchivePath(String sourceImageArchivePath) {
        this.sourceImageArchivePath = sourceImageArchivePath;
    }

    @JsonIgnore
    public String getSourceImagePath() {
        if (StringUtils.isBlank(sourceImageArchivePath)) {
            return sourceImageName;
        } else {
            return Paths.get(sourceImageArchivePath, sourceImageName).toString();
        }
    }

    public String getSourceSampleRef() {
        return sourceSampleRef;
    }

    public void setSourceSampleRef(String sourceSampleRef) {
        this.sourceSampleRef = sourceSampleRef;
    }

    public String getSourceRelatedImageRefId() {
        return sourceRelatedImageRefId;
    }

    public void setSourceRelatedImageRefId(String sourceRelatedImageRefId) {
        this.sourceRelatedImageRefId = sourceRelatedImageRefId;
    }

    public String getSourceImageURL() {
        return sourceImageURL;
    }

    public void setSourceImageURL(String sourceImageURL) {
        this.sourceImageURL = sourceImageURL;
    }

    public String getSourceSearchablePNG() {
        return sourceSearchablePNG;
    }

    public void setSourceSearchablePNG(String sourceSearchablePNG) {
        this.sourceSearchablePNG = sourceSearchablePNG;
    }

    public String getSourceImageStack() {
        return sourceImageStack;
    }

    public void setSourceImageStack(String sourceImageStack) {
        this.sourceImageStack = sourceImageStack;
    }

    public String getSourceScreenImage() {
        return sourceScreenImage;
    }

    public void setSourceScreenImage(String sourceScreenImage) {
        this.sourceScreenImage = sourceScreenImage;
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

    public boolean isMirrored() {
        return mirrored;
    }

    public void setMirrored(boolean mirrored) {
        this.mirrored = mirrored;
    }

    @JsonIgnore
    public long getGradientAreaGap() {
        return gradientAreaGap == null ? -1 : gradientAreaGap;
    }

    public void setGradientAreaGap(long gradientAreaGap) {
        this.gradientAreaGap = gradientAreaGap >= 0 ? gradientAreaGap : null;
    }

    @JsonIgnore
    public long getHighExpressionArea() {
        return highExpressionArea == null ?  -1 : highExpressionArea;
    }

    public void setHighExpressionArea(long highExpressionArea) {
        this.highExpressionArea = highExpressionArea >= 0 ?  highExpressionArea : null;
    }

    @JsonIgnore
    public long getNegativeScore() {
        return GradientAreaGapUtils.calculateNegativeScore(gradientAreaGap, highExpressionArea);
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

    public Double getNormalizedScore() {
        if (normalizedGapScore != null) {
            return normalizedGapScore;
        } else {
            return artificialShapeScore != null ? artificialShapeScore : getMatchingPixels();
        }
    }

    public boolean matches(ColorMIPSearchMatchMetadata that) {
        return this.getId() != null && this.getId().equals(that.getSourceId()) &&
                this.getSourceId() != null && this.getSourceId().equals(that.getId());
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

    public <T extends ColorMIPSearchMatchMetadata> void copyTo(T that) {
        super.copyTo(that);
        // copy source image
        that.setSourceId(this.sourceId);
        that.setSourcePublishedName(this.sourcePublishedName);
        that.setSourceLibraryName(this.sourceLibraryName);
        that.setSourceCdmPath(this.sourceCdmPath);
        that.setSourceImageType(this.sourceImageType);
        that.setSourceImageName(this.sourceImageName);
        that.setSourceImageArchivePath(this.sourceImageArchivePath);
        that.setSourceSearchablePNG(this.sourceSearchablePNG);
        that.setSourceImageStack(this.sourceImageStack);
        that.setSourceScreenImage(this.sourceScreenImage);
        // copy score attributes
        that.setMatchingPixels(this.getMatchingPixels());
        that.setMatchingRatio(this.getMatchingRatio());
        that.setMirrored(this.isMirrored());
        that.setGradientAreaGap(this.getGradientAreaGap());
        that.setHighExpressionArea(this.getHighExpressionArea());
        that.setNormalizedGapScore(this.getNormalizedGapScore());
        that.setArtificialShapeScore(this.getArtificialShapeScore());
        that.setSearchablePNG(this.getSearchablePNG());
        that.setImageStack(this.getImageStack());
        that.setScreenImage(this.getScreenImage());
    }

    @Override
    protected Consumer<String> attributeValueHandler(String attrName) {
        if (StringUtils.isBlank(attrName)) {
            return (attrValue) -> {}; // do nothing handler
        } else {
            /*
             * This relies on the ordering of the JSON file in which all source identifiers
             * occur before any matched identifier and the matchedId occurs before any other
             * match identifer such as matchedLibraryName, matchedPublishedName.
             */
            switch (attrName) {
                case "matchedId":
                    return (attrValue) -> {
                        copyIdentifiersToSourceIdentifiers();
                        setId(attrValue);
                    };
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
                default:
                    return super.attributeValueHandler(attrName);
            }
        }
    }

    private void copyIdentifiersToSourceIdentifiers() {
        this.sourceId = getId();
        this.sourceCdmPath = getCdmPath();
        this.sourcePublishedName = getPublishedName();
        this.sourceLibraryName = getLibraryName();
        this.sourceImageName = getImageName();
        this.sourceImageArchivePath = getImageArchivePath();
        this.sourceImageType = getImageType();
    }

    @Override
    protected String mapAttr(String attrName) {
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
        }
        return super.mapAttr(attrName);
    }

}
