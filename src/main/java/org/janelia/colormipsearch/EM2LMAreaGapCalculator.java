package org.janelia.colormipsearch;

import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.imageprocessing.ImageArray;
import org.janelia.colormipsearch.imageprocessing.ImageProcessing;
import org.janelia.colormipsearch.imageprocessing.ImageTransformation;
import org.janelia.colormipsearch.imageprocessing.LImage;
import org.janelia.colormipsearch.imageprocessing.TriFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This can be used to adjust the score for an EM mask against an LM (segmented) library
 */
class EM2LMAreaGapCalculator {

    private static class GradientAreaComputeContext {
        private static final ImageTransformation MIRROR_IMAGE = ImageTransformation.horizontalMirror();

        final LImage pattern;
        final LImage patternRegions; // pix(x,y) = 1 if pattern.pix(x,y) is set
        final LImage overExpressedRegions; // pix(x,y) = 1 if there's too much expression surrounding x,y

        private GradientAreaComputeContext(LImage pattern, LImage patternRegions, LImage overExpressedRegions) {
            this.pattern = pattern;
            this.patternRegions = patternRegions;
            this.overExpressedRegions = overExpressedRegions;
        }

        private GradientAreaComputeContext horizontalMirror() {
            return new GradientAreaComputeContext(
                    pattern.mapi(MIRROR_IMAGE),
                    patternRegions.mapi(MIRROR_IMAGE),
                    overExpressedRegions.mapi(MIRROR_IMAGE)
            );
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(EM2LMAreaGapCalculator.class);

    private final ImageProcessing labelsClearing;
    private final ImageProcessing negativeRadiusDilation;
    private final ImageProcessing toSignalTransformation;
    private final TriFunction<LImage, LImage, GradientAreaComputeContext, Long> gapCalculator;
    private final Function<MIPImage, BiFunction<MIPImage, MIPImage, Long>> gradientAreaCalculatorForMask;

    EM2LMAreaGapCalculator(int maskThreshold, int negativeRadius, boolean mirrorMask) {
        this.labelsClearing = ImageProcessing.create(
                ImageTransformation.clearRegion((x, y) -> x < 330 && y < 100 || x >= 950 && y < 85));
        this.negativeRadiusDilation = labelsClearing
                .mask(maskThreshold)
                .maxFilter(negativeRadius);
        this.toSignalTransformation = ImageProcessing.create()
                .toGray16()
                .toSignal();
        this.gapCalculator = createGapCalculator(maskThreshold);
        this.gradientAreaCalculatorForMask = createGradientAreaCalculatorForMask(mirrorMask);
    }

    BiFunction<MIPImage, MIPImage, Long> getGradientAreaCalculator(MIPImage maskImage) {
        return gradientAreaCalculatorForMask.apply(maskImage);
    }

    long calculateGradientAreaAdjustment(MIPImage image1, MIPImage imageGradient1, MIPImage image2, MIPImage imageGradient2) {
        if (imageGradient1 != null) {
            return calculateAdjustedScore(image2, image1, imageGradient1);
        } else if (imageGradient2 != null) {
            return calculateAdjustedScore(image1, image2, imageGradient2);
        } else {
            return -1;
        }
    }

    private TriFunction<LImage, LImage, GradientAreaComputeContext, Long> createGapCalculator(int maskThreshold) {
        return (inputImage, inputGradientImage, gradientAreaComputeContext) -> {
            long startTime = System.currentTimeMillis();
            LImage gaps = LImage.combine3(
                    LImage.combine2(
                            gradientAreaComputeContext.patternRegions,
                            inputGradientImage,
                            (p1, p2) -> p1 * p2),
                    gradientAreaComputeContext.pattern,
                    negativeRadiusDilation.applyTo(inputImage),
                    GradientAreaGapUtils.PIXEL_GAP_OP
            );
            LImage overExpressedRegions = LImage.combine2(
                    gradientAreaComputeContext.overExpressedRegions,
                    labelsClearing.applyTo(inputImage),
                    (p1, p2) -> {
                        if (p1 == 0) {
                            return 0;
                        } else {
                            int r2 = (p2 >>> 16) & 0xff;
                            int g2 = (p2 >>> 8) & 0xff;
                            int b2 = p2 & 0xff;
                            if (r2 > maskThreshold || g2 > maskThreshold || b2 > maskThreshold) {
                                return 1;
                            } else {
                                return 0;
                            }
                        }
                    });
            long gradientArea = gaps.fold(0L, (p, s) -> s + p);
            long tooMuchExpression = overExpressedRegions.fold(0L, (p, s) -> s + p);
            long areaGapScore = gradientArea + tooMuchExpression / 2;
            LOG.trace("Area gap score -> {} computed in {}ms", areaGapScore, System.currentTimeMillis() - startTime);
            return areaGapScore;
        };
    }

    private long calculateAdjustedScore(MIPImage maskMIP, MIPImage inputMIP, MIPImage inputGradientMIP) {
        long startTimestamp = System.currentTimeMillis();
        try {
            LOG.debug("Calculate area gap between {} - {} using {}", maskMIP, inputMIP, inputGradientMIP);
            return getGradientAreaCalculator(maskMIP).apply(inputMIP, inputGradientMIP);
        } finally {
            LOG.debug("Finished calculating area gap between {} - {} using {} in {}ms", maskMIP, inputMIP, inputGradientMIP, System.currentTimeMillis()-startTimestamp);
        }
    }

    private Function<MIPImage, BiFunction<MIPImage, MIPImage, Long>> createGradientAreaCalculatorForMask(boolean mirrorMask) {
        return (MIPImage maskMIP) -> {
            GradientAreaComputeContext gradientAreaComputeContext = prepareContextForCalculatingGradientAreaGap(maskMIP.imageArray);
            return (MIPImage inputMIP, MIPImage inputGradientMIP) -> {
                LImage inputImage = LImage.create(inputMIP.imageArray);
                LImage inputGradientImage = LImage.create(inputGradientMIP.imageArray);

                long areaGap = gapCalculator.apply(inputImage, inputGradientImage, gradientAreaComputeContext);
                if (mirrorMask) {
                    long mirrorAreaGap = gapCalculator.apply(inputImage, inputGradientImage, gradientAreaComputeContext.horizontalMirror());
                    if (mirrorAreaGap < areaGap) {
                        return mirrorAreaGap;
                    }
                }
                return areaGap;
            };
        };
    }

    private GradientAreaComputeContext prepareContextForCalculatingGradientAreaGap(ImageArray patternImageArray) {
        LImage patternImage = LImage.create(patternImageArray);
        LImage overExpressedRegionsInPatternImage = LImage.combine2(
                LImage.createDilatedImage(patternImageArray, 60),
                LImage.createDilatedImage(patternImageArray, 20),
                (p1, p2) -> p2 != -16777216 ? -16777216 : p1
        );
        return new GradientAreaComputeContext(
                patternImage,
                toSignalTransformation.applyTo(patternImage),
                toSignalTransformation.applyTo(overExpressedRegionsInPatternImage)
        );
    }

}
