package org.janelia.colormipsearch;

import java.io.Serializable;

import javax.annotation.Nullable;

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

    private final MIPInfo maskMIP;
    private final MIPInfo libraryMIP;
    private final int matchingPixels;
    private final double matchingPixelsPct;
    private final boolean isMatch;
    private final boolean isError;
    private long gradientAreaGap;
    private boolean mirrorHasBetterMatch;

    ColorMIPSearchResult(MIPInfo maskMIP, MIPInfo libraryMIP, int matchingPixels, double matchingPixelsPct, boolean isMatch, boolean isError) {
        Preconditions.checkArgument(maskMIP != null);
        Preconditions.checkArgument(libraryMIP != null);
        this.maskMIP = maskMIP;
        this.libraryMIP = libraryMIP;
        this.matchingPixels = matchingPixels;
        this.matchingPixelsPct = matchingPixelsPct;
        this.isMatch = isMatch;
        this.isError = isError;
    }

    String getLibraryId() {
        return libraryMIP.id;
    }

    String getMaskId() {
        return maskMIP.id;
    }

    int getMatchingPixels() {
        return matchingPixels;
    }

    boolean isMatch() {
        return isMatch;
    }

    boolean isError() {
        return isError;
    }

    ColorMIPSearchResult applyGradientAreaGap(@Nullable AreaGap gradientAreaGap) {
        if (gradientAreaGap == null) {
            this.gradientAreaGap = -1;
            this.mirrorHasBetterMatch = false;
        } else {
            this.gradientAreaGap = gradientAreaGap.value;
            this.mirrorHasBetterMatch = gradientAreaGap.mirrorHasBetterMatch;
        }
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
        srMetadata.imageUrl = maskMIP.imageURL;
        srMetadata.thumbnailUrl = maskMIP.thumbnailURL;
        srMetadata.imageArchivePath = libraryMIP.archivePath;
        srMetadata.imageType = libraryMIP.type;
        srMetadata.imageName = libraryMIP.imagePath;
        srMetadata.imageVolumeSize = libraryMIP.volumeSize;
        srMetadata.imageShapeScore = libraryMIP.shapeScore;

        srMetadata.matchedId = getMaskId();
        srMetadata.matchedPublishedName = maskMIP.publishedName;
        srMetadata.matchedImageArchivePath = maskMIP.archivePath;
        srMetadata.matchedImageName = maskMIP.imagePath;
        srMetadata.matchedImageType = maskMIP.type;
        srMetadata.matchedImageVolumeSize  = maskMIP.volumeSize;
        srMetadata.matchedImageShapeScore = maskMIP.shapeScore;
        srMetadata.addAttr("Library", maskMIP.libraryName);
        srMetadata.addAttr("PublishedName", maskMIP.publishedName);
        srMetadata.setMatchingPixels(matchingPixels);
        srMetadata.setMatchingPixelsPct(matchingPixelsPct);
        srMetadata.setGradientAreaGap(gradientAreaGap);
        return srMetadata;
    }

    ColorMIPSearchResultMetadata perMaskMetadata() {
        ColorMIPSearchResultMetadata srMetadata = new ColorMIPSearchResultMetadata();
        srMetadata.id = getMaskId();
        srMetadata.libraryName = maskMIP.libraryName;
        srMetadata.publishedName = maskMIP.publishedName;
        srMetadata.imageUrl = libraryMIP.imageURL;
        srMetadata.thumbnailUrl = libraryMIP.thumbnailURL;
        srMetadata.imageArchivePath = maskMIP.archivePath;
        srMetadata.imageName = maskMIP.imagePath;
        srMetadata.imageType = maskMIP.type;
        srMetadata.imageVolumeSize  = maskMIP.volumeSize;
        srMetadata.imageShapeScore = maskMIP.shapeScore;

        srMetadata.matchedId = getLibraryId();
        srMetadata.matchedPublishedName = libraryMIP.publishedName;
        srMetadata.matchedImageArchivePath = libraryMIP.archivePath;
        srMetadata.matchedImageName = libraryMIP.imagePath;
        srMetadata.matchedImageType = libraryMIP.type;
        srMetadata.matchedImageVolumeSize  = libraryMIP.volumeSize;
        srMetadata.matchedImageShapeScore = libraryMIP.shapeScore;
        srMetadata.addAttr("Library", libraryMIP.libraryName);
        srMetadata.addAttr("PublishedName", libraryMIP.publishedName);
        srMetadata.setMatchingPixels(matchingPixels);
        srMetadata.setMatchingPixelsPct(matchingPixelsPct);
        srMetadata.setGradientAreaGap(gradientAreaGap);
        return srMetadata;
    }

}
