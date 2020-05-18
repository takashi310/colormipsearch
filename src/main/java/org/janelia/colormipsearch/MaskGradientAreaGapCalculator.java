package org.janelia.colormipsearch;

import org.janelia.colormipsearch.imageprocessing.ImageProcessing;
import org.janelia.colormipsearch.imageprocessing.ImageTransformation;
import org.janelia.colormipsearch.imageprocessing.LImage;
import org.janelia.colormipsearch.imageprocessing.LImageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This calculates the gradient area gap between an EM mask and an LM (segmented) image.
 */
public class MaskGradientAreaGapCalculator {

    private static final Logger LOG = LoggerFactory.getLogger(MaskGradientAreaGapCalculator.class);

    public static MaskGradientAreaGapCalculatorProvider createMaskGradientAreaGapCalculatorProvider(int maskThreshold,
                                                                                                    int negativeRadius,
                                                                                                    boolean mirrorMask) {
        ImageProcessing clearLabels = ImageProcessing.create(
                ImageTransformation.clearRegion((x, y) -> x < 330 && y < 100 || x >= 950 && y < 85));
        ImageProcessing signalRegions = ImageProcessing.create()
                .toGray16()
                .toSignalRegions();
        ImageProcessing negativeRadiusDilation = clearLabels.mask(maskThreshold).maxFilter(negativeRadius);
        return (MIPImage maskMIPImage) -> {
            long startTime = System.currentTimeMillis();
            LImage maskImage = LImageUtils.create(maskMIPImage.imageArray);
            LImage overExpressedRegionsInPatternImage = LImageUtils.lazyCombine2(
                    maskImage.mapi(ImageTransformation.maxFilter(60)).reduce(), // eval immediately
                    maskImage.mapi(ImageTransformation.maxFilter(20)).reduce(), // eval immediately
                    (p1s, p2s) -> p2s.get() != -16777216 ? -16777216 : p1s.get()
            );
            MaskGradientAreaGapCalculator maskGradientAreaGapCalculator = new MaskGradientAreaGapCalculator(
                    maskImage,
                    signalRegions.applyTo(maskImage),
                    signalRegions.applyTo(overExpressedRegionsInPatternImage),
                    maskThreshold,
                    mirrorMask,
                    clearLabels,
                    negativeRadiusDilation
            );

            LOG.debug("Created gradient area gap calculator for mask {} in {}ms", maskMIPImage, System.currentTimeMillis() - startTime);
            return maskGradientAreaGapCalculator;
        };
    }

    private final LImage mask;
    private final LImage maskRegions; // pix(x,y) = 1 if pattern.pix(x,y) is set
    private final LImage overExpressedRegionsInMask; // pix(x,y) = 1 if there's too much expression surrounding x,y
    private int maskThreshold;
    private final boolean withMaskMirroring;
    private final ImageProcessing labelsClearing;
    private final ImageProcessing negativeRadiusDilation;

    private MaskGradientAreaGapCalculator(LImage mask,
                                          LImage maskRegions,
                                          LImage overExpressedRegionsInMask,
                                          int maskThreshold,
                                          boolean withMaskMirroring,
                                          ImageProcessing labelsClearing,
                                          ImageProcessing negativeRadiusDilation) {
        this.mask = mask;
        this.maskRegions = maskRegions;
        this.overExpressedRegionsInMask = overExpressedRegionsInMask;
        this.maskThreshold = maskThreshold;
        this.withMaskMirroring = withMaskMirroring;
        this.labelsClearing = labelsClearing;
        this.negativeRadiusDilation = negativeRadiusDilation;
    }

    public long calculateMaskAreaGap(MIPImage inputMIPImage, MIPImage inputGradientMIPImage, MIPImage inputZGapMIPImage) {
        long gaStartTime = System.currentTimeMillis();
        LOG.trace("Calculate gradient area gap for {} with {}, {}", inputMIPImage, inputGradientMIPImage, inputZGapMIPImage);
        LImage inputImage = LImageUtils.create(inputMIPImage.imageArray);
        LImage inputGradientImage = LImageUtils.create(inputGradientMIPImage.imageArray);
        LImage inputZGapImage = inputZGapMIPImage != null
                ? LImageUtils.create(inputZGapMIPImage.imageArray)
                : negativeRadiusDilation.applyTo(inputImage).reduce(); // eval immediately

        long areaGap = calculateAreaGap(inputImage, inputGradientImage, inputZGapImage, ImageTransformation.identity());
        LOG.trace("Finished gradient area gap for {} with {}, {} = {} in {}ms",
                inputMIPImage, inputGradientMIPImage, inputZGapMIPImage, areaGap, System.currentTimeMillis() - gaStartTime);

        if (withMaskMirroring) {
            long mirrorAreaGap = calculateAreaGap(inputImage, inputGradientImage, inputZGapImage, ImageTransformation.horizontalMirror());
            LOG.trace("Finished mirrored gradient area gap for {} with {}, {} = {} in {}ms",
                    inputMIPImage, inputGradientMIPImage, inputZGapMIPImage, mirrorAreaGap, System.currentTimeMillis() - gaStartTime);
            if (mirrorAreaGap < areaGap) {
                return mirrorAreaGap;
            }
        }
        return areaGap;
    }

    private long calculateAreaGap(LImage inputImage, LImage inputGradientImage, LImage inputZGapImage, ImageTransformation maskTransformation) {
        long startTime = System.currentTimeMillis();
        LImage gaps = LImageUtils.lazyCombine3(
                LImageUtils.combine2(
                        maskRegions.mapi(maskTransformation),
                        inputGradientImage,
                        (p1, p2) -> p1 * p2),
                mask.mapi(maskTransformation),
                inputZGapImage.mapi(maskTransformation),
                GradientAreaGapUtils.PIXEL_GAP_OP
        );
        LImage overExpressedRegions = LImageUtils.lazyCombine2(
                overExpressedRegionsInMask.mapi(maskTransformation),
                labelsClearing.applyTo(inputImage),
                (p1s, p2s) -> {
                    int p1 = p1s.get();
                    if (p1 == 0) {
                        return 0;
                    } else {
                        int p2 = p2s.get();
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
        long gradientAreaGap = gaps.fold(0L, (p, s) -> s + p);
        long tooMuchExpression = overExpressedRegions.fold(0L, (p, s) -> s + p);
        long areaGapScore = gradientAreaGap + tooMuchExpression / 2;
        LOG.trace("Area gap score {} computed in {}ms", areaGapScore, System.currentTimeMillis() - startTime);
        return areaGapScore;
    }

}
