package org.janelia.colormipsearch.api;

import org.janelia.colormipsearch.api.imageprocessing.ImageArray;

@FunctionalInterface
public interface MaskGradientAreaGapCalculatorProvider {
    MaskGradientAreaGapCalculator createMaskGradientAreaGapCalculator(ImageArray maskImage);
}
