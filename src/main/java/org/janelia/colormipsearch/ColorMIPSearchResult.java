package org.janelia.colormipsearch;

import java.io.Serializable;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * The result of comparing a search mask against a given image.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class ColorMIPSearchResult implements Serializable {

    private final String patternId;
    private final String patternFilepath;
    private final String libraryId;
    private final String libraryFilepath;
    private final int matchingSlices;
    private final double matchingSlicesPct;
    private final boolean isMatch;
    private final boolean isError;

    public ColorMIPSearchResult(String patternId, String patternFilepath, String libraryId, String libraryFilepath, int matchingSlices, double matchingSlicesPct, boolean isMatch, boolean isError) {
        this.patternId = patternId;
        this.patternFilepath = patternFilepath;
        this.libraryId = libraryId;
        this.libraryFilepath = libraryFilepath;
        this.matchingSlices = matchingSlices;
        this.matchingSlicesPct = matchingSlicesPct;
        this.isMatch = isMatch;
        this.isError = isError;
    }

    public String getPatternId() {
        return patternId;
    }

    public String getPatternFilepath() {
        return patternFilepath;
    }

    public String getLibraryId() {
        return libraryId;
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
                .append("patternId", patternId)
                .append("patternFilepath", patternFilepath)
                .append("libraryId", libraryId)
                .append("libraryFilepath", libraryFilepath)
                .append("matchingSlices", matchingSlices)
                .append("matchingSlicesPct", matchingSlicesPct)
                .append("isMatch", isMatch)
                .append("isError", isError)
                .toString();
    }
}
