package org.janelia.colormipsearch.model;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

public enum PPPScreenshotType {
    RAW(FileType.SignalMip, null),
    MASKED_RAW(FileType.SignalMipMasked, null),
    SKEL(FileType.SignalMipMaskedSkel, null),
    CH(FileType.CDMBest, FileType.CDMBestThumbnail), // for a CH file we generate a reference to the MIP and a reference to the thumbnail
    CH_SKEL(FileType.CDMSkel, null);

    private final FileType fileType;
    private final FileType thumbnailFileType;

    PPPScreenshotType(FileType fileType, @Nullable FileType thumbnailFileType) {
        this.fileType = fileType;
        this.thumbnailFileType = thumbnailFileType;
    }

    static PPPScreenshotType findScreenshotType(String imageName) {
        for (PPPScreenshotType t : values()) {
            if (t.fileType.hasFileSuffix() && imageName.endsWith(t.fileType.getFileSuffix())) {
                return t;
            }
        }
        return null;
    }

    public FileType getFileType() {
        return fileType;
    }

    public FileType getThumbnailFileType() {
        return thumbnailFileType;
    }

    public boolean hasThumnail() {
        return thumbnailFileType != null;
    }
}
