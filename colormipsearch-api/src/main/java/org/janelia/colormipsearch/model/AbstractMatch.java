package org.janelia.colormipsearch.model;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractMatch<I extends AbstractNeuronMetadata> {
    private I matchedImage;
    private boolean mirrored; // if true the matchedImage was mirrored
    private Map<FileType, FileData> matchFiles = new HashMap<>(); // match specific files

    public I getMatchedImage() {
        return matchedImage;
    }

    public void setMatchedImage(I matchedImage) {
        this.matchedImage = matchedImage;
    }

    public boolean isMirrored() {
        return mirrored;
    }

    public void setMirrored(boolean mirrored) {
        this.mirrored = mirrored;
    }

    public Map<FileType, FileData> getMatchFiles() {
        return matchFiles;
    }

    public void setMatchFiles(Map<FileType, FileData> matchFiles) {
        this.matchFiles = matchFiles;
    }
}
