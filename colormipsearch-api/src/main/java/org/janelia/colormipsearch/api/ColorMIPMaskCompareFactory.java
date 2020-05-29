package org.janelia.colormipsearch.api;

import org.janelia.colormipsearch.api.imageprocessing.ImageArray;

public class ColorMIPMaskCompareFactory {
    public static ColorMIPMaskCompare createMaskComparator(
            ImageArray maskImage,
            int maskThreshold,
            boolean mirrorMask,
            int searchThreshold,
            double zTolerance,
            int xyShift) {
        return new FColorMIPMaskCompare(
                maskImage,
                maskThreshold,
                mirrorMask,
                null,
                0,
                false,
                searchThreshold,
                zTolerance,
                xyShift
        );
    }

}
