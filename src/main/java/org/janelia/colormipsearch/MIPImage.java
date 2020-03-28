package org.janelia.colormipsearch;

public class MIPImage {
    final MIPInfo mipInfo;
    final ImageArray imageArray;

    MIPImage(MIPInfo mipInfo, ImageArray imageArray) {
        this.mipInfo = mipInfo;
        this.imageArray = imageArray;
    }
}
