package org.janelia.colormipsearch;

import java.io.Serializable;

/**
 * The result of comparing a search mask against a given image.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class MaskSearchResult implements Serializable {

    private final String filepath;
    private final int matchingSlices;
    private final double matchingSlicesPct;
    private final boolean isMatch;

    public MaskSearchResult(String filepath, int matchingSlices, double matchingSlicesPct, boolean isMatch) {
        this.filepath = filepath;
        this.matchingSlices = matchingSlices;
        this.matchingSlicesPct = matchingSlicesPct;
        this.isMatch = isMatch;
    }

    public String getFilepath() {
        return filepath;
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
}
