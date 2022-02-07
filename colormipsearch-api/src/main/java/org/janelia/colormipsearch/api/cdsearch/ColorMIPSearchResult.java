package org.janelia.colormipsearch.api.cdsearch;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;

/**
 * The result of comparing a search mask against a given image.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class ColorMIPSearchResult implements Serializable {

    private final MIPMetadata maskMIP;
    private final MIPMetadata libraryMIP;
    private final int matchingPixels;
    private final double matchingRatio;
    private final long gradientAreaGap;
    private final long highExpressionArea;
    private final boolean mirrored;
    private final boolean matchAbovePctPositivePixels;
    private final boolean errorsFound;

    public ColorMIPSearchResult(MIPMetadata maskMIP,
                                MIPMetadata libraryMIP,
                                ColorMIPMatchScore cdsScore,
                                boolean matchAbovePctPositivePixels,
                                boolean errorsFound) {
        this.maskMIP = maskMIP;
        this.libraryMIP = libraryMIP;
        this.matchingPixels = cdsScore.getMatchingPixNum();
        this.matchingRatio = cdsScore.getMatchingPixNumToMaskRatio();
        this.gradientAreaGap = cdsScore.getGradientAreaGap();
        this.highExpressionArea = cdsScore.getHighExpressionArea();
        this.mirrored = cdsScore.isMirrored();
        this.matchAbovePctPositivePixels = matchAbovePctPositivePixels;
        this.errorsFound = errorsFound;
    }

    public String getLibraryId() {
        return libraryMIP.getId();
    }

    public String getMaskId() {
        return maskMIP.getId();
    }

    public int getMatchingPixels() {
        return matchingPixels;
    }

    public boolean isMirrored() {
        return mirrored;
    }

    public boolean isMatch() {
        return !errorsFound && matchAbovePctPositivePixels;
    }

    public boolean hasErrors() {
        return errorsFound;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        ColorMIPSearchResult that = (ColorMIPSearchResult) o;

        return new EqualsBuilder()
                .append(matchingPixels, that.matchingPixels)
                .append(maskMIP, that.maskMIP)
                .append(libraryMIP, that.libraryMIP)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(maskMIP)
                .append(libraryMIP)
                .append(matchingPixels)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("maskMIP", maskMIP)
                .append("libraryMIP", libraryMIP)
                .append("matchingPixels", matchingPixels)
                .append("matchingPixelsPct", matchingRatio)
                .append("mirrored", mirrored)
                .append("areaGap", gradientAreaGap)
                .append("highExpressionArea", highExpressionArea)
                .append("matchAbovePctPositivePixels", matchAbovePctPositivePixels)
                .append("errorsFound", errorsFound)
                .toString();
    }

    public ColorMIPSearchMatchMetadata perLibraryMetadata() {
        ColorMIPSearchMatchMetadata srMetadata = new ColorMIPSearchMatchMetadata();
        srMetadata.setSourceId(getLibraryId());
        srMetadata.setSourceLibraryName(libraryMIP.getLibraryName());
        srMetadata.setSourcePublishedName(libraryMIP.getPublishedName());
        srMetadata.setSourceCdmPath(libraryMIP.getCdmPath());
        srMetadata.setSourceImageArchivePath(libraryMIP.getImageArchivePath());
        srMetadata.setSourceImageType(libraryMIP.getImageType());
        srMetadata.setSourceImageName(libraryMIP.getImageName());
        srMetadata.setSourceSampleRef(libraryMIP.getSampleRef());
        srMetadata.setSourceRelatedImageRefId(libraryMIP.getRelatedImageRefId());
        srMetadata.setSourceImageURL(libraryMIP.getImageURL());
        srMetadata.setSourceSearchablePNG(libraryMIP.getSearchablePNG());
        srMetadata.setSourceImageStack(libraryMIP.getImageStack());

        srMetadata.setId(getMaskId());
        srMetadata.setLibraryName(maskMIP.getLibraryName());
        srMetadata.setPublishedName(maskMIP.getPublishedName());
        srMetadata.setCdmPath(maskMIP.getCdmPath());
        srMetadata.setImageArchivePath(maskMIP.getImageArchivePath());
        srMetadata.setImageName(maskMIP.getImageName());
        srMetadata.setImageType(maskMIP.getImageType());
        srMetadata.setSampleRef(maskMIP.getSampleRef());
        srMetadata.setRelatedImageRefId(maskMIP.getRelatedImageRefId());
        srMetadata.setImageURL(maskMIP.getImageURL());
        srMetadata.setThumbnailURL(maskMIP.getThumbnailURL());
        srMetadata.setSearchablePNG(maskMIP.getSearchablePNG());
        srMetadata.setImageStack(maskMIP.getImageStack());

        srMetadata.setSlideCode(maskMIP.getSlideCode());
        srMetadata.setObjective(maskMIP.getObjective());
        srMetadata.setNeuronType(maskMIP.getNeuronType());
        srMetadata.setNeuronInstance(maskMIP.getNeuronInstance());
        srMetadata.setGender(maskMIP.getGender());
        srMetadata.setAnatomicalArea(maskMIP.getAnatomicalArea());
        srMetadata.setAlignmentSpace(maskMIP.getAlignmentSpace());
        srMetadata.setChannel(maskMIP.getChannel());
        srMetadata.setMountingProtocol(maskMIP.getMountingProtocol());
        srMetadata.setRelatedImageRefId(maskMIP.getRelatedImageRefId());
        srMetadata.copyVariantsFrom(maskMIP);

        srMetadata.setMatchingPixels(matchingPixels);
        srMetadata.setMatchingRatio(matchingRatio);
        srMetadata.setMirrored(mirrored);
        srMetadata.setGradientAreaGap(gradientAreaGap);
        srMetadata.setHighExpressionArea(highExpressionArea);
        return srMetadata;
    }

    public ColorMIPSearchMatchMetadata perMaskMetadata() {
        ColorMIPSearchMatchMetadata srMetadata = new ColorMIPSearchMatchMetadata();
        srMetadata.setSourceId(getMaskId());
        srMetadata.setSourceLibraryName(maskMIP.getLibraryName());
        srMetadata.setSourcePublishedName(maskMIP.getPublishedName());
        srMetadata.setSourceCdmPath(maskMIP.getCdmPath());
        srMetadata.setSourceImageArchivePath(maskMIP.getImageArchivePath());
        srMetadata.setSourceImageName(maskMIP.getImageName());
        srMetadata.setSourceImageType(maskMIP.getImageType());
        srMetadata.setSourceSampleRef(maskMIP.getSampleRef());
        srMetadata.setSourceRelatedImageRefId(maskMIP.getRelatedImageRefId());
        srMetadata.setSourceImageURL(maskMIP.getImageURL());
        srMetadata.setSourceSearchablePNG(maskMIP.getSearchablePNG());
        srMetadata.setSourceImageStack(maskMIP.getImageStack());

        srMetadata.setId(getLibraryId());
        srMetadata.setLibraryName(libraryMIP.getLibraryName());
        srMetadata.setPublishedName(libraryMIP.getPublishedName());
        srMetadata.setCdmPath(libraryMIP.getCdmPath());
        srMetadata.setImageArchivePath(libraryMIP.getImageArchivePath());
        srMetadata.setImageName(libraryMIP.getImageName());
        srMetadata.setImageType(libraryMIP.getImageType());
        srMetadata.setSampleRef(libraryMIP.getSampleRef());
        srMetadata.setRelatedImageRefId(libraryMIP.getRelatedImageRefId());
        srMetadata.setImageURL(libraryMIP.getImageURL());
        srMetadata.setThumbnailURL(libraryMIP.getThumbnailURL());
        srMetadata.setSearchablePNG(libraryMIP.getSearchablePNG());
        srMetadata.setImageStack(libraryMIP.getImageStack());

        srMetadata.setSlideCode(libraryMIP.getSlideCode());
        srMetadata.setObjective(libraryMIP.getObjective());
        srMetadata.setNeuronType(libraryMIP.getNeuronType());
        srMetadata.setNeuronInstance(libraryMIP.getNeuronInstance());
        srMetadata.setGender(libraryMIP.getGender());
        srMetadata.setAnatomicalArea(libraryMIP.getAnatomicalArea());
        srMetadata.setAlignmentSpace(libraryMIP.getAlignmentSpace());
        srMetadata.setChannel(libraryMIP.getChannel());
        srMetadata.setMountingProtocol(libraryMIP.getMountingProtocol());
        srMetadata.setRelatedImageRefId(libraryMIP.getRelatedImageRefId());
        srMetadata.copyVariantsFrom(libraryMIP);

        srMetadata.setMatchingPixels(matchingPixels);
        srMetadata.setMatchingRatio(matchingRatio);
        srMetadata.setMirrored(mirrored);
        srMetadata.setGradientAreaGap(gradientAreaGap);
        srMetadata.setHighExpressionArea(highExpressionArea);
        return srMetadata;
    }

}
