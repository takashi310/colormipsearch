package org.janelia.colormipsearch.api.imageprocessing;

import ij.ImagePlus;

enum ImageType {
    UNKNOWN(-1),
    GRAY8(ImagePlus.GRAY8),
    GRAY16(ImagePlus.GRAY16),
    RGB(ImagePlus.COLOR_RGB);

    private int ipType;

    ImageType(int ipType) {
        this.ipType = ipType;
    }

    static ImageType fromImagePlusType(int ipType) {
        for (ImageType it : ImageType.values()) {
            if (it.ipType == ipType) {
                return it;
            }
        }
        throw new IllegalArgumentException("Invalid image type: " + ipType);
    }
}
