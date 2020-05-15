package org.janelia.colormipsearch;

@FunctionalInterface
public interface MaskGradientAreaGapCalculatorProvider {
    MaskGradientAreaGapCalculator createMaskGradientAreaGapCalculator(MIPImage maskMIPImage);
}
