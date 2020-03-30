package org.janelia.colormipsearch;

import org.janelia.colormipsearch.imageprocessing.ImageArray;

public class MIPImage {
    final MIPInfo mipInfo;
    final ImageArray imageArray;

    MIPImage(MIPInfo mipInfo, ImageArray imageArray) {
        this.mipInfo = mipInfo;
        this.imageArray = imageArray;
    }
}
