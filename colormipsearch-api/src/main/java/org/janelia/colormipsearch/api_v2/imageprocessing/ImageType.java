package org.janelia.colormipsearch.api_v2.imageprocessing;

import ij.ImagePlus;

/**
 * ImageType specifies the pixel type such as:
 *   - gray - 8 bits pixel
 *   - gray - 16 bits pixel
 *   - RGB - 24 bits pixel
 *      R - bits 0-7
 *      G - bits 8-15
 *      B - bits 16-23
 *      this type is used for ARGB pixels as well where alpha is in the most significant byte position - 24-32
 */
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
