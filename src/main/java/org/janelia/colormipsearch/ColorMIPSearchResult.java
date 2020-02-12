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
public class ColorMIPSearchResult implements Serializable {

    final MinimalColorDepthMIP maskMIP;
    final MinimalColorDepthMIP libraryMIP;
    final int matchingSlices;
    final double matchingSlicesPct;
    final boolean isMatch;
    final boolean isError;

    public ColorMIPSearchResult(MinimalColorDepthMIP maskMIP, MinimalColorDepthMIP libraryMIP, int matchingSlices, double matchingSlicesPct, boolean isMatch, boolean isError) {
        Preconditions.checkArgument(maskMIP != null);
        Preconditions.checkArgument(libraryMIP != null);
        this.maskMIP = maskMIP;
        this.libraryMIP = libraryMIP;
        this.matchingSlices = matchingSlices;
        this.matchingSlicesPct = matchingSlicesPct;
        this.isMatch = isMatch;
        this.isError = isError;
    }

    public String getLibraryId() {
        return libraryMIP.id;
    }

    public String getMaskId() {
        return maskMIP.id;
    }

    public int getMatchingSlices() {
        return matchingSlices;
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
                .append("isMatch", isMatch)
                .append("isError", isError)
                .toString();
    }

}
