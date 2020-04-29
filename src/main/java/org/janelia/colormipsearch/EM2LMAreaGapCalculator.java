package org.janelia.colormipsearch;

import java.util.function.BiFunction;
import java.util.function.Function;

import org.janelia.colormipsearch.imageprocessing.ImageArray;
import org.janelia.colormipsearch.imageprocessing.ImageProcessing;
import org.janelia.colormipsearch.imageprocessing.ImageTransformation;
import org.janelia.colormipsearch.imageprocessing.LImage;
import org.janelia.colormipsearch.imageprocessing.LImageUtils;
import org.janelia.colormipsearch.imageprocessing.QuadFunction;
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
        final ImageTransformation negativeRadiusImageTransformation;

        private GradientAreaComputeContext(LImage pattern, LImage patternRegions, LImage overExpressedRegions, ImageTransformation negativeRadiusImageTransformation) {
            this.pattern = pattern;
            this.patternRegions = patternRegions;
            this.overExpressedRegions = overExpressedRegions;
            this.negativeRadiusImageTransformation = negativeRadiusImageTransformation;
        }

        private GradientAreaComputeContext horizontalMirror() {
            return new GradientAreaComputeContext(
                    pattern.mapi(MIRROR_IMAGE),
                    patternRegions.mapi(MIRROR_IMAGE),
                    overExpressedRegions.mapi(MIRROR_IMAGE),
                    ImageTransformation.horizontalMirror()
            );
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(EM2LMAreaGapCalculator.class);

    private final ImageProcessing labelsClearing;
    private final ImageProcessing negativeRadiusDilation;
    private final ImageProcessing toSignalTransformation;
    private final QuadFunction<LImage, LImage, LImage, GradientAreaComputeContext, Long> gapCalculator;
    private final Function<MIPImage, TriFunction<MIPImage, MIPImage, MIPImage, Long>> gradientAreaCalculatorForMask;

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

    TriFunction<MIPImage, MIPImage, MIPImage, Long> getGradientAreaCalculator(MIPImage maskImage) {
        return gradientAreaCalculatorForMask.apply(maskImage);
    }

    long calculateGradientAreaAdjustment(MIPImage image1, MIPImage imageGradient1, MIPImage image2, MIPImage imageGradient2) {
        // in this case we always calculate the ZGap image
        if (imageGradient1 != null) {
            return calculateAdjustedScore(image2, image1, imageGradient1, null);
        } else if (imageGradient2 != null) {
            return calculateAdjustedScore(image1, image2, imageGradient2, null);
        } else {
            return -1;
        }
    }

    private QuadFunction<LImage, LImage, LImage, GradientAreaComputeContext, Long> createGapCalculator(int maskThreshold) {
        return (inputImage, inputGradientImage, inputZGapImage, gradientAreaComputeContext) -> {
            long startTime = System.currentTimeMillis();
            LImage gaps = LImageUtils.combine3(
                    LImageUtils.combine2(
                            gradientAreaComputeContext.patternRegions,
                            inputGradientImage,
                            (p1, p2) -> p1 * p2),
                    gradientAreaComputeContext.pattern,
                    inputZGapImage.mapi(gradientAreaComputeContext.negativeRadiusImageTransformation),
                    GradientAreaGapUtils.PIXEL_GAP_OP
            );
            LImage overExpressedRegions = LImageUtils.combine2(
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
            LOG.trace("Area gap score {} computed in {}ms", areaGapScore, System.currentTimeMillis() - startTime);
            return areaGapScore;
        };
    }

    private long calculateAdjustedScore(MIPImage maskMIP, MIPImage inputMIP, MIPImage inputGradientMIP, MIPImage inputZGapMIP) {
        long startTimestamp = System.currentTimeMillis();
        try {
            LOG.debug("Calculate area gap between {} - {} using {}", maskMIP, inputMIP, inputGradientMIP);
            return getGradientAreaCalculator(maskMIP).apply(inputMIP, inputGradientMIP, inputZGapMIP);
        } finally {
            LOG.debug("Finished calculating area gap between {} - {} using {} in {}ms", maskMIP, inputMIP, inputGradientMIP, System.currentTimeMillis() - startTimestamp);
        }
    }

    private Function<MIPImage, TriFunction<MIPImage, MIPImage, MIPImage, Long>> createGradientAreaCalculatorForMask(boolean mirrorMask) {
        return (MIPImage maskMIP) -> {
            long startTime = System.currentTimeMillis();
            GradientAreaComputeContext gradientAreaComputeContext = prepareContextForCalculatingGradientAreaGap(maskMIP.imageArray);
            LOG.debug("Prepare gradient area gap context for {} in {}ms", maskMIP, System.currentTimeMillis() - startTime);
            return (MIPImage inputMIP, MIPImage inputGradientMIP, MIPImage inputZGapMIP) -> {
                long gaStartTime = System.currentTimeMillis();
                LOG.trace("Calculate gradient area gap for {} with {}, {}", inputMIP, inputGradientMIP, inputZGapMIP);
                LImage inputImage = LImageUtils.create(inputMIP.imageArray);
                LImage inputGradientImage = LImageUtils.create(inputGradientMIP.imageArray);
                LImage inputZGapImage = inputZGapMIP != null
                        ? LImageUtils.create(inputZGapMIP.imageArray)
                        : negativeRadiusDilation.applyTo(inputImage);

                long areaGap = gapCalculator.apply(inputImage, inputGradientImage, inputZGapImage, gradientAreaComputeContext);
                LOG.trace("Finished gradient area gap for {} with {}, {} in {}ms",
                        inputMIP, inputGradientMIP, inputZGapMIP, System.currentTimeMillis()-gaStartTime);
                if (mirrorMask) {
                    long mirrorAreaGap = gapCalculator.apply(inputImage, inputGradientImage, inputZGapImage, gradientAreaComputeContext.horizontalMirror());
                    LOG.trace("Finished mirrored gradient area gap for {} with {}, {} in {}ms",
                            inputMIP, inputGradientMIP, inputZGapMIP, System.currentTimeMillis()-gaStartTime);
                    if (mirrorAreaGap < areaGap) {
                        return mirrorAreaGap;
                    }
                }
                return areaGap;
            };
        };
    }

    private GradientAreaComputeContext prepareContextForCalculatingGradientAreaGap(ImageArray patternImageArray) {
        LImage patternImage = LImageUtils.create(patternImageArray);
        LImage overExpressedRegionsInPatternImage = LImageUtils.combine2(
                LImageUtils.create(patternImageArray).mapi(ImageTransformation.maxFilter(60)),
                LImageUtils.create(patternImageArray).mapi(ImageTransformation.maxFilter(20)),
                (p1, p2) -> p2 != -16777216 ? -16777216 : p1
        );
        return new GradientAreaComputeContext(
                patternImage,
                toSignalTransformation.applyTo(patternImage),
                toSignalTransformation.applyTo(overExpressedRegionsInPatternImage).reduce(),
                ImageTransformation.horizontalMirror()
        );
    }

}
