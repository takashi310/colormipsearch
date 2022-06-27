package org.janelia.colormipsearch.model;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractMatch<M extends AbstractNeuronMetadata, I extends AbstractNeuronMetadata> {
    private M maskImage;
    private I matchedImage;
    private boolean mirrored; // if true the matchedImage was mirrored
    private Map<FileType, FileData> matchFiles = new HashMap<>(); // match specific files

    public M getMaskImage() {
        return maskImage;
    }

    public void setMaskImage(M maskImage) {
        this.maskImage = maskImage;
    }

    public void resetMaskImage() {
        this.maskImage = null;
    }

    public I getMatchedImage() {
        return matchedImage;
    }

    public void setMatchedImage(I matchedImage) {
        this.matchedImage = matchedImage;
    }

    public void resetMatchedImage() {
        this.matchedImage = null;
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

    public void setMatchFileData(FileType t, FileData fd) {
        if (fd != null) {
            matchFiles.put(t, fd);
        } else {
            matchFiles.remove(t);
        }
    }
}
