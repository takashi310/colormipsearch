package org.janelia.colormipsearch.model;

import java.util.Arrays;
import java.util.List;

public enum PPPScreenshotType {
    RAW(FileType.SignalMip),
    MASKED_RAW(FileType.SignalMipMasked),
    SKEL(FileType.SignalMipMaskedSkel),
    CH(FileType.CDMBest, FileType.CDMBestThumbnail), // for a CH file we generate a reference to the MIP and a reference to the thumbnail
    CH_SKEL(FileType.CDMSkel);

    private final List<FileType> fileTypes;

    PPPScreenshotType(FileType... fileType) {
        this.fileTypes = Arrays.asList(fileType);
    }

    static PPPScreenshotType findScreenshotType(String imageName) {
        for (PPPScreenshotType t : values()) {
            boolean matchingFileType = t.fileTypes.stream()
                    .filter(FileType::hasFileSuffix)
                    .anyMatch(ft -> imageName.endsWith(ft.getFileSuffix()));
            if (matchingFileType) {
                return t;
            }
        }
        return null;
    }

    public List<FileType> getFileTypes() {
        return fileTypes;
    }
}
