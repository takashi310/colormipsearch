package org.janelia.colormipsearch;

import java.io.Serializable;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * The result of comparing a search mask against a given image.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
class ColorMIPSearchResult implements Serializable {

    static class AreaGap {
        final long value;
        final boolean mirrorHasBetterMatch;

        AreaGap(long value, boolean mirrorHasBetterMatch) {
            this.value = value;
            this.mirrorHasBetterMatch = mirrorHasBetterMatch;
        }
    }

    final MIPInfo maskMIP;
    final MIPInfo libraryMIP;
    final int matchingSlices;
    final double matchingSlicesPct;
    final boolean isMatch;
    final boolean isError;
    final long areaGap;
    final boolean mirrorHasBetterMatch;

    ColorMIPSearchResult(MIPInfo maskMIP, MIPInfo libraryMIP, int matchingSlices, double matchingSlicesPct, boolean isMatch, AreaGap areaGap, boolean isError) {
        Preconditions.checkArgument(maskMIP != null);
        Preconditions.checkArgument(libraryMIP != null);
        this.maskMIP = maskMIP;
        this.libraryMIP = libraryMIP;
        this.matchingSlices = matchingSlices;
        this.matchingSlicesPct = matchingSlicesPct;
        this.isMatch = isMatch;
        this.areaGap = areaGap.value;
        this.mirrorHasBetterMatch = areaGap.mirrorHasBetterMatch;
        this.isError = isError;
    }

    String getLibraryId() {
        return libraryMIP.id;
    }

    String getMaskId() {
        return maskMIP.id;
    }

    int getMatchingSlices() {
        return matchingSlices;
    }

    boolean isMatch() {
        return isMatch;
    }

    boolean isError() {
        return isError;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        ColorMIPSearchResult that = (ColorMIPSearchResult) o;

        return new EqualsBuilder()
                .append(matchingSlices, that.matchingSlices)
                .append(maskMIP, that.maskMIP)
                .append(libraryMIP, that.libraryMIP)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(maskMIP)
                .append(libraryMIP)
                .append(matchingSlices)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("maskMIP", maskMIP)
                .append("libraryMIP", libraryMIP)
                .append("matchingSlices", matchingSlices)
                .append("matchingSlicesPct", matchingSlicesPct)
                .append("areaGap", areaGap)
                .append("mirrorHasBetterMatch", mirrorHasBetterMatch)
                .append("isMatch", isMatch)
                .append("isError", isError)
                .toString();
    }

    ColorMIPSearchResultMetadata perLibraryMetadata() {
        ColorMIPSearchResultMetadata srMetadata = new ColorMIPSearchResultMetadata();
        srMetadata.id = getLibraryId();
        srMetadata.libraryName = libraryMIP.libraryName;
        srMetadata.publishedName = libraryMIP.publishedName;
        srMetadata.matchedId = getMaskId();
        srMetadata.imageUrl = maskMIP.imageURL;
        srMetadata.thumbnailUrl = maskMIP.thumbnailURL;
        srMetadata.addAttr("Library", maskMIP.libraryName);
        srMetadata.addAttr("PublishedName", maskMIP.publishedName);
        srMetadata.addAttr("Matched slices", String.valueOf(matchingSlices));
        srMetadata.addAttr("Score", String.valueOf(matchingSlicesPct));
        return srMetadata;
    }

    ColorMIPSearchResultMetadata perMaskMetadata() {
        ColorMIPSearchResultMetadata srMetadata = new ColorMIPSearchResultMetadata();
        srMetadata.id = getMaskId();
        srMetadata.libraryName = maskMIP.libraryName;
        srMetadata.publishedName = maskMIP.publishedName;
        srMetadata.matchedId = getLibraryId();
        srMetadata.imageUrl = libraryMIP.imageURL;
        srMetadata.thumbnailUrl = libraryMIP.thumbnailURL;
        srMetadata.addAttr("Library", libraryMIP.libraryName);
        srMetadata.addAttr("PublishedName", libraryMIP.publishedName);
        srMetadata.addAttr("Matched slices", String.valueOf(matchingSlices));
        srMetadata.addAttr("Score", String.valueOf(matchingSlicesPct));
        return srMetadata;
    }

}
