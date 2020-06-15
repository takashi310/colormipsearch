package org.janelia.colormipsearch.api;

import org.janelia.colormipsearch.api.imageprocessing.ImageArray;

@FunctionalInterface
public interface MaskGradientAreaGapCalculatorProvider {
    /**
     * this method is essentially a constructor for a NaskGradientAreaGapCalculator
     * that encapsulates the provided mask.
     *
     * @param maskImage encapsulated mask image
     * @return a gradient area gap calculator for the given mask
     */
    MaskGradientAreaGapCalculator createMaskGradientAreaGapCalculator(ImageArray maskImage);
}
