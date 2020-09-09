package org.janelia.colormipsearch.api.gradienttools;

import org.janelia.colormipsearch.api.imageprocessing.ImageArray;

@FunctionalInterface
public interface MaskNegativeScoresCalculatorProvider {
    /**
     * this method is essentially a constructor for a NaskGradientAreaGapCalculator
     * that encapsulates the provided mask.
     *
     * @param maskImage encapsulated mask image
     * @return a gradient area gap calculator for the given mask
     */
    MaskNegativeScoresCalculator createMaskNegativeScoresCalculator(ImageArray maskImage);
}
