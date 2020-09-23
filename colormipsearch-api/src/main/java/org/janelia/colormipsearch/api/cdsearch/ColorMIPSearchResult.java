package org.janelia.colormipsearch.api.cdsearch;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.api.cdmips.MIPIdentifier;
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
    private final boolean isMatch;
    private final boolean isError;
    private long gradientAreaGap;

    public ColorMIPSearchResult(MIPMetadata maskMIP, MIPMetadata libraryMIP, int matchingPixels, double matchingRatio, boolean isMatch, boolean isError) {
        this.maskMIP = maskMIP;
        this.libraryMIP = libraryMIP;
        this.matchingPixels = matchingPixels;
        this.matchingRatio = matchingRatio;
        this.isMatch = isMatch;
        this.isError = isError;
        this.gradientAreaGap = -1;
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

    public boolean isMatch() {
        return isMatch;
    }

    public boolean isError() {
        return isError;
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
                .append("areaGap", gradientAreaGap)
                .append("isMatch", isMatch)
                .append("isError", isError)
                .toString();
    }

    public ColorMIPSearchMatchMetadata perLibraryMetadata() {
        ColorMIPSearchMatchMetadata srMetadata = new ColorMIPSearchMatchMetadata();
        srMetadata.setSourceId(getLibraryId());
        srMetadata.setSourceLibraryName(libraryMIP.getLibraryName());
        srMetadata.setSourcePublishedName(libraryMIP.getPublishedName());
        srMetadata.setSourceImageArchivePath(libraryMIP.getImageArchivePath());
        srMetadata.setSourceImageType(libraryMIP.getImageType());
        srMetadata.setSourceImageName(libraryMIP.getImageName());
        srMetadata.setSourceImageURL(libraryMIP.getImageURL());

        srMetadata.setImageURL(maskMIP.getImageURL());
        srMetadata.setThumbnailURL(maskMIP.getThumbnailURL());

        srMetadata.setId(getMaskId());
        srMetadata.setLibraryName(maskMIP.getLibraryName());
        srMetadata.setPublishedName(maskMIP.getPublishedName());
        srMetadata.setCdmPath(maskMIP.getCdmPath());
        srMetadata.setImageArchivePath(maskMIP.getImageArchivePath());
        srMetadata.setImageName(maskMIP.getImageName());
        srMetadata.setImageType(maskMIP.getImageType());

        srMetadata.setSlideCode(maskMIP.getSlideCode());
        srMetadata.setObjective(maskMIP.getObjective());
        srMetadata.setGender(maskMIP.getGender());
        srMetadata.setAnatomicalArea(maskMIP.getAnatomicalArea());
        srMetadata.setAlignmentSpace(maskMIP.getAlignmentSpace());
        srMetadata.setChannel(maskMIP.getChannel());
        srMetadata.setMountingProtocol(maskMIP.getMountingProtocol());
        srMetadata.setRelatedImageRefId(maskMIP.getRelatedImageRefId());

        srMetadata.setMatchingPixels(matchingPixels);
        srMetadata.setMatchingRatio(matchingRatio);
        srMetadata.setGradientAreaGap(gradientAreaGap);
        return srMetadata;
    }

    public ColorMIPSearchMatchMetadata perMaskMetadata() {
        ColorMIPSearchMatchMetadata srMetadata = new ColorMIPSearchMatchMetadata();
        srMetadata.setSourceId(getMaskId());
        srMetadata.setSourceLibraryName(maskMIP.getLibraryName());
        srMetadata.setSourcePublishedName(maskMIP.getPublishedName());
        srMetadata.setSourceImageArchivePath(maskMIP.getImageArchivePath());
        srMetadata.setSourceImageName(maskMIP.getImageName());
        srMetadata.setSourceImageType(maskMIP.getImageType());
        srMetadata.setSourceImageURL(maskMIP.getImageURL());

        srMetadata.setImageURL(libraryMIP.getImageURL());
        srMetadata.setThumbnailURL(libraryMIP.getThumbnailURL());

        srMetadata.setId(getLibraryId());
        srMetadata.setLibraryName(libraryMIP.getLibraryName());
        srMetadata.setPublishedName(libraryMIP.getPublishedName());
        srMetadata.setCdmPath(libraryMIP.getCdmPath());
        srMetadata.setImageArchivePath(libraryMIP.getImageArchivePath());
        srMetadata.setImageName(libraryMIP.getImageName());
        srMetadata.setImageType(libraryMIP.getImageType());

        srMetadata.setSlideCode(libraryMIP.getSlideCode());
        srMetadata.setObjective(libraryMIP.getObjective());
        srMetadata.setGender(libraryMIP.getGender());
        srMetadata.setAnatomicalArea(libraryMIP.getAnatomicalArea());
        srMetadata.setAlignmentSpace(libraryMIP.getAlignmentSpace());
        srMetadata.setChannel(libraryMIP.getChannel());
        srMetadata.setMountingProtocol(libraryMIP.getMountingProtocol());
        srMetadata.setRelatedImageRefId(libraryMIP.getRelatedImageRefId());

        srMetadata.setMatchingPixels(matchingPixels);
        srMetadata.setMatchingRatio(matchingRatio);
        srMetadata.setGradientAreaGap(gradientAreaGap);
        return srMetadata;
    }

}
