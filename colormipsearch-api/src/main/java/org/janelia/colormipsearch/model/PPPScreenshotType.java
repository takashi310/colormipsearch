package org.janelia.colormipsearch.model;

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
            if (t.fileType.hasFileSuffix() && imageName.endsWith(t.fileType.getFileSuffix())) {
                return t;
            }
        }
        return null;
    }

    public FileType getFileType() {
        return fileType;
    }
}
