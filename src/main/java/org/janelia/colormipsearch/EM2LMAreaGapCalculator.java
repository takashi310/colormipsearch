package org.janelia.colormipsearch;

import java.util.function.BiFunction;
import java.util.function.Function;

import org.janelia.colormipsearch.imageprocessing.ImageArray;
import org.janelia.colormipsearch.imageprocessing.ImageProcessing;
import org.janelia.colormipsearch.imageprocessing.ImageTransformation;
import org.janelia.colormipsearch.imageprocessing.LImage;
import org.janelia.colormipsearch.imageprocessing.QuadFunction;
import org.janelia.colormipsearch.imageprocessing.TriFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This can be used to adjust the score for an EM mask against an LM (segmented) library
 */
class EM2LMAreaGapCalculator {

    private static class GradientAreaComputeContext {
        LImage pattern;
        LImage overExpressedRegions; // pix(x,y) = 1 if there's too much expression surrounding x,y
        LImage patternRegions; // pix(x,y) = 1 if pattern.pix(x,y) is set
    }

    private static final Logger LOG = LoggerFactory.getLogger(EM2LMAreaGapCalculator.class);
    private static final ImageTransformation MIRROR_IMAGE = ImageTransformation.horizontalMirror();

    private final ImageProcessing labelsClearing;
    private final ImageProcessing negativeRadiusDilation;
    private final ImageProcessing toSignalTransformation;
    private final QuadFunction<LImage, LImage, GradientAreaComputeContext, ImageTransformation, Long> gapCalculator;
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

    private QuadFunction<LImage, LImage, GradientAreaComputeContext, ImageTransformation, Long> createGapCalculator(int maskThreshold) {
        return (inputImage, inputGradientImage, gradientAreaComputeContext, patternTransformation) -> {
            long startTime = System.currentTimeMillis();
            LImage gaps = LImage.combine3(
                    LImage.combine2(
                            gradientAreaComputeContext.patternRegions.mapi(patternTransformation),
                            inputGradientImage,
                            (p1, p2) -> p1 * p2),
                    gradientAreaComputeContext.pattern.mapi(patternTransformation),
                    negativeRadiusDilation.applyTo(inputImage),
                    GradientAreaGapUtils.PIXEL_GAP_OP
            );
            LImage overExpressedRegions = LImage.combine2(
                    gradientAreaComputeContext.overExpressedRegions.mapi(patternTransformation),
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
            LOG.debug("Area gap score -> {} computed in {}ms", areaGapScore, System.currentTimeMillis() - startTime);
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

    private TriFunction<ImageArray, ImageArray, ImageArray, Long> scoreAdjustmentProcessing(boolean mirrorMask) {
        return (patternImageArray, libraryImageArray, libraryGradientImageArray) -> {
            GradientAreaComputeContext gradientAreaComputeContext = prepareContextForCalculatingGradientAreaGap(patternImageArray);

            LImage libraryImage = LImage.create(libraryImageArray);
            LImage libraryGradientImage = LImage.create(libraryGradientImageArray);

            long areaGap = gapCalculator.apply(libraryImage, libraryGradientImage, gradientAreaComputeContext, ImageTransformation.IDENTITY);
            if (mirrorMask) {
                long mirrorAreaGap = gapCalculator.apply(libraryImage, libraryGradientImage, gradientAreaComputeContext, MIRROR_IMAGE);
                if (mirrorAreaGap < areaGap) {
                    return mirrorAreaGap;
                }
            }
            return areaGap;
        };
    }

    private Function<MIPImage, BiFunction<MIPImage, MIPImage, Long>> createGradientAreaCalculatorForMask(boolean mirrorMask) {
        return (MIPImage maskMIP) -> {
            GradientAreaComputeContext gradientAreaComputeContext = prepareContextForCalculatingGradientAreaGap(maskMIP.imageArray);
            return (MIPImage inputMIP, MIPImage inputGradientMIP) -> {
                LImage inputImage = LImage.create(inputMIP.imageArray);
                LImage inputGradientImage = LImage.create(inputGradientMIP.imageArray);

                long areaGap = gapCalculator.apply(inputImage, inputGradientImage, gradientAreaComputeContext, ImageTransformation.IDENTITY);
                if (mirrorMask) {
                    long mirrorAreaGap = gapCalculator.apply(inputImage, inputGradientImage, gradientAreaComputeContext, MIRROR_IMAGE);
                    if (mirrorAreaGap < areaGap) {
                        return mirrorAreaGap;
                    }
                }
                return areaGap;
            };
        };
    }

    private GradientAreaComputeContext prepareContextForCalculatingGradientAreaGap(ImageArray patternImageArray) {
        LImage dilated60pxPatternImage = LImage.createDilatedImage(patternImageArray, 60);
        LImage dilated20pxPatternImage = LImage.createDilatedImage(patternImageArray, 20);

        LImage overExpressedRegionsInPatternImage = toSignalTransformation.applyTo(
                LImage.combine2(
                        dilated60pxPatternImage,
                        dilated20pxPatternImage,
                        (p1, p2) -> p2 != -16777216 ? -16777216 : p1
                ));

        LImage patternImage = LImage.create(patternImageArray);
        LImage patternSignalImage = toSignalTransformation.applyTo(patternImage);
        GradientAreaComputeContext gradientAreaComputeContext = new GradientAreaComputeContext();
        gradientAreaComputeContext.pattern = patternImage;
        gradientAreaComputeContext.patternRegions = patternSignalImage;
        gradientAreaComputeContext.overExpressedRegions = toSignalTransformation.applyTo(overExpressedRegionsInPatternImage);
        return gradientAreaComputeContext;
    }


}
