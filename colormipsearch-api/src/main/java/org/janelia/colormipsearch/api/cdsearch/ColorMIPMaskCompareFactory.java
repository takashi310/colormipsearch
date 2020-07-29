package org.janelia.colormipsearch.api.cdsearch;

import org.janelia.colormipsearch.api.imageprocessing.ImageArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for a color depth search comparator.
 */
public class ColorMIPMaskCompareFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ColorMIPMaskCompareFactory.class);

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
        LOG.debug("Create mask comparator with mirrorMask={}, maskThreshold={}, dataThreshold={}, zTolerance={}, xyShift={}",
                mirrorMask, maskThreshold, searchThreshold, zTolerance, xyShift);
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
