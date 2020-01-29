package org.janelia.colormipsearch;

import java.io.Serializable;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * The result of comparing a search mask against a given image.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class ColorMIPSearchResult implements Serializable {

    private final String patternFilepath;
    private final String libraryFilepath;
    private final int matchingSlices;
    private final double matchingSlicesPct;
    private final boolean isMatch;
    private final boolean isError;

    public ColorMIPSearchResult(String patternFilepath, String libraryFilepath, int matchingSlices, double matchingSlicesPct, boolean isMatch, boolean isError) {
        this.patternFilepath = patternFilepath;
        this.libraryFilepath = libraryFilepath;
        this.matchingSlices = matchingSlices;
        this.matchingSlicesPct = matchingSlicesPct;
        this.isMatch = isMatch;
        this.isError = isError;
    }

    public String getPatternFilepath() {
        return patternFilepath;
    }

    public String getLibraryFilepath() {
        return libraryFilepath;
    }

    public int getMatchingSlices() {
        return matchingSlices;
    }

    public double getMatchingSlicesPct() {
        return matchingSlicesPct;
    }

    public boolean isMatch() {
        return isMatch;
    }

    public boolean isError() {
        return isError;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("patternFilepath", patternFilepath)
                .append("libraryFilepath", libraryFilepath)
                .append("matchingSlices", matchingSlices)
                .append("matchingSlicesPct", matchingSlicesPct)
                .append("isMatch", isMatch)
                .append("isError", isError)
                .toString();
    }
}
