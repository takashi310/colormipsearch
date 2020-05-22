package org.janelia.colormipsearch.tools;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.api.imageprocessing.ImageArray;

public class MIPImage {
    final MIPInfo mipInfo;
    final ImageArray imageArray;

    MIPImage(MIPInfo mipInfo, ImageArray imageArray) {
        this.mipInfo = mipInfo;
        this.imageArray = imageArray;
    }

    public ImageArray getImageArray() {
        return imageArray;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("mipInfo", mipInfo)
                .toString();
    }
}
