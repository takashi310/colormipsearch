package org.janelia.colormipsearch.api;

import org.janelia.colormipsearch.api.imageprocessing.ImageArray;

/**
 * Factory for a color depth search comparator.
 */
public class ColorMIPMaskCompareFactory {
    /**
     * Create a color depth mask comparator that encapsulates the given mask.
     *
     * @param maskImage mask for the color depth search comparator
     * @param maskThreshold mask threshold
     * @param mirrorMask flag whether to use mirroring
     * @param searchThreshold data threshold
     * @param zTolerance z - gap tolerance - sometimes called pixel color fluctuation
     * @param xyShift - x-y translation for searching for a match
     * @return a color depth search comparator for the given mask
     */
    public static ColorMIPMaskCompare createMaskComparator(
            ImageArray maskImage,
            int maskThreshold,
            boolean mirrorMask,
            int searchThreshold,
            double zTolerance,
            int xyShift) {
        return new ArrayColorMIPMaskCompare(
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
