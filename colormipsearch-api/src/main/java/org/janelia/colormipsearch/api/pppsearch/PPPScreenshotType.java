package org.janelia.colormipsearch.api.pppsearch;

import org.janelia.colormipsearch.api.FileType;

public enum PPPScreenshotType {
    RAW(FileType.SignalMip),
    MASKED_RAW(FileType.SignalMipMasked),
    SKEL(FileType.SignalMipMaskedSkel),
    CH(FileType.ColorDepthMip),
    CH_SKEL(FileType.ColorDepthMipSkel);

    private FileType fileType;

    PPPScreenshotType(FileType fileType) {
        this.fileType = fileType;
    }

    static PPPScreenshotType findScreenshotType(String imageName) {
        for (PPPScreenshotType t : values()) {
            if (imageName.endsWith(t.fileType.getFullSuffix())) {
                return t;
            }
        }
        return null;
    }

    FileType getFileType() {
        return fileType;
    }
}
