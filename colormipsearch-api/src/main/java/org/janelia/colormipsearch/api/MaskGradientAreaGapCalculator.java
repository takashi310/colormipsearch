package org.janelia.colormipsearch.api;

import org.janelia.colormipsearch.api.imageprocessing.ColorTransformation;
import org.janelia.colormipsearch.api.imageprocessing.ImageArray;
import org.janelia.colormipsearch.api.imageprocessing.ImageProcessing;
import org.janelia.colormipsearch.api.imageprocessing.ImageTransformation;
import org.janelia.colormipsearch.api.imageprocessing.LImage;
import org.janelia.colormipsearch.api.imageprocessing.LImageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This calculates the gradient area gap between an EM mask and an LM (segmented) image.
 */
public class MaskGradientAreaGapCalculator {

    private static final Logger LOG = LoggerFactory.getLogger(MaskGradientAreaGapCalculator.class);
    private static final int GAP_THRESHOLD = 3;

    public static MaskGradientAreaGapCalculatorProvider createMaskGradientAreaGapCalculatorProvider(int maskThreshold,
                                                                                                    int negativeRadius,
                                                                                                    boolean mirrorMask) {
        ImageProcessing clearLabels = ImageProcessing.create(
                ImageTransformation.clearRegion((x, y) -> x < 330 && y < 100 || x >= 950 && y < 85));
        ImageProcessing negativeRadiusDilation = clearLabels.mask(maskThreshold).maxFilter(negativeRadius);
        return (ImageArray maskImageArray) -> {
            long startTime = System.currentTimeMillis();
            LImage maskImage = LImageUtils.create(maskImageArray);
            LImage maskForRegionsWithTooMuchExpression = LImageUtils.lazyCombine2(
                    maskImage.mapi(ImageTransformation.maxFilter(60)).reduce(), // eval immediately
                    maskImage.mapi(ImageTransformation.maxFilter(20)).reduce(), // eval immediately
                    (p1s, p2s) -> p2s.get() != -16777216 ? -16777216 : p1s.get() // mask pixels from the 60x image if they are present in the 20x image
            );
            MaskGradientAreaGapCalculator maskGradientAreaGapCalculator = new MaskGradientAreaGapCalculator(
                    maskImage,
                    maskImage.map(ColorTransformation.toGray16(false)).map(ColorTransformation.toSignalRegions(2)),
                    maskForRegionsWithTooMuchExpression.map(ColorTransformation.toGray16(false)).map(ColorTransformation.toSignalRegions(0)),
                    maskThreshold,
                    mirrorMask,
                    clearLabels,
                    negativeRadiusDilation
            );

            LOG.debug("Created gradient area gap calculator for mask in {}ms", System.currentTimeMillis() - startTime);
            return maskGradientAreaGapCalculator;
        };
    }

    private final LImage mask;
    private final LImage maskIntensityValues;
    private final LImage maskForUnwantedExpression; // pix(x,y) = 1 if there's too much expression surrounding x,y
    private int maskThreshold;
    private final boolean withMaskMirroring;
    private final ImageProcessing labelsClearing;
    private final ImageProcessing negativeRadiusDilation;

    private MaskGradientAreaGapCalculator(LImage mask,
                                          LImage maskIntensityValues,
                                          LImage maskForUnwantedExpression,
                                          int maskThreshold,
                                          boolean withMaskMirroring,
                                          ImageProcessing labelsClearing,
                                          ImageProcessing negativeRadiusDilation) {
        this.mask = mask;
        this.maskIntensityValues = maskIntensityValues;
        this.maskForUnwantedExpression = maskForUnwantedExpression;
        this.maskThreshold = maskThreshold;
        this.withMaskMirroring = withMaskMirroring;
        this.labelsClearing = labelsClearing;
        this.negativeRadiusDilation = negativeRadiusDilation;
    }

    public long calculateMaskAreaGap(ImageArray inputImageArray,
                                     ImageArray inputGradientImageArray,
                                     ImageArray inputZGapImageArray) {
        LImage inputImage = LImageUtils.create(inputImageArray);
        LImage inputGradientImage = LImageUtils.create(inputGradientImageArray);
        LImage inputZGapImage = inputZGapImageArray != null
                ? LImageUtils.create(inputZGapImageArray)
                : negativeRadiusDilation.applyTo(inputImage).reduce(); // eval immediately

        long areaGap = calculateAreaGap(inputImage, inputGradientImage, inputZGapImage, ImageTransformation.identity());

        if (withMaskMirroring) {
            long mirrorAreaGap = calculateAreaGap(inputImage, inputGradientImage, inputZGapImage, ImageTransformation.horizontalMirror());
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
                        maskIntensityValues.mapi(maskTransformation),
                        inputGradientImage,
                        (p1, p2) -> p1 * p2),
                mask.mapi(maskTransformation),
                inputZGapImage.mapi(maskTransformation),
                GradientAreaGapUtils.PIXEL_GAP_OP
        );
        LImage overExpressedRegions = LImageUtils.lazyCombine2(
                maskForUnwantedExpression.mapi(maskTransformation),
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
        long gradientAreaGap = gaps.fold(0L, (p, s) -> p > GAP_THRESHOLD ? s + p : s);
        long tooMuchExpression = overExpressedRegions.fold(0L, (p, s) -> s + p);
        long areaGapScore = gradientAreaGap + tooMuchExpression / 2;
        LOG.trace("Area gap score {} computed in {}ms", areaGapScore, System.currentTimeMillis() - startTime);
        return areaGapScore;
    }

}
