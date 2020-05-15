package org.janelia.colormipsearch;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * The result of comparing a search mask against a given image.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class ColorMIPSearchResult implements Serializable {

    private final MIPInfo maskMIP;
    private final MIPInfo libraryMIP;
    private final int matchingPixels;
    private final double matchingPixelsPct;
    private final boolean isMatch;
    private final boolean isError;
    private long gradientAreaGap;

    ColorMIPSearchResult(MIPInfo maskMIP, MIPInfo libraryMIP, int matchingPixels, double matchingPixelsPct, boolean isMatch, boolean isError) {
        this.maskMIP = maskMIP;
        this.libraryMIP = libraryMIP;
        this.matchingPixels = matchingPixels;
        this.matchingPixelsPct = matchingPixelsPct;
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

    ColorMIPSearchResult applyGradientAreaGap(long gradientAreaGap) {
        this.gradientAreaGap = gradientAreaGap;
        return this;
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
                .append("matchingPixelsPct", matchingPixelsPct)
                .append("areaGap", gradientAreaGap)
                .append("isMatch", isMatch)
                .append("isError", isError)
                .toString();
    }

    public ColorMIPSearchResultMetadata perLibraryMetadata() {
        ColorMIPSearchResultMetadata srMetadata = new ColorMIPSearchResultMetadata();
        srMetadata.setId(getLibraryId());
        srMetadata.setLibraryName(libraryMIP.getLibraryName());
        srMetadata.setPublishedName(libraryMIP.getPublishedName());
        srMetadata.setImageUrl(maskMIP.getImageURL());
        srMetadata.setThumbnailUrl(maskMIP.getThumbnailURL());
        srMetadata.setImageArchivePath(libraryMIP.getArchivePath());
        srMetadata.setImageType(libraryMIP.getType());
        srMetadata.setImageName(libraryMIP.getImagePath());

        srMetadata.setMatchedId(getMaskId());
        srMetadata.setMatchedPublishedName(maskMIP.getPublishedName());
        srMetadata.setMatchedImageArchivePath(maskMIP.getArchivePath());
        srMetadata.setMatchedImageName(maskMIP.getImagePath());
        srMetadata.setMatchedImageType(maskMIP.getType());
        maskMIP.iterateAttrs(srMetadata::addAttr);
        srMetadata.addAttr("Library", maskMIP.getLibraryName());
        srMetadata.setMatchingPixels(matchingPixels);
        srMetadata.setMatchingPixelsPct(matchingPixelsPct);
        srMetadata.setGradientAreaGap(gradientAreaGap);
        return srMetadata;
    }

    public ColorMIPSearchResultMetadata perMaskMetadata() {
        ColorMIPSearchResultMetadata srMetadata = new ColorMIPSearchResultMetadata();
        srMetadata.setId(getMaskId());
        srMetadata.setLibraryName(maskMIP.getLibraryName());
        srMetadata.setPublishedName(maskMIP.getPublishedName());
        srMetadata.setImageUrl(libraryMIP.getImageURL());
        srMetadata.setThumbnailUrl(libraryMIP.getThumbnailURL());
        srMetadata.setImageArchivePath(maskMIP.getArchivePath());
        srMetadata.setImageName(maskMIP.getImagePath());
        srMetadata.setImageType(maskMIP.getType());

        srMetadata.setMatchedId(getLibraryId());
        srMetadata.setMatchedPublishedName(libraryMIP.getPublishedName());
        srMetadata.setMatchedImageArchivePath(libraryMIP.getArchivePath());
        srMetadata.setMatchedImageName(libraryMIP.getImagePath());
        srMetadata.setMatchedImageType(libraryMIP.getType());
        libraryMIP.iterateAttrs(srMetadata::addAttr); // add all attributes from libraryMIP
        srMetadata.addAttr("Library", libraryMIP.getLibraryName());
        srMetadata.setMatchingPixels(matchingPixels);
        srMetadata.setMatchingPixelsPct(matchingPixelsPct);
        srMetadata.setGradientAreaGap(gradientAreaGap);
        return srMetadata;
    }

}
