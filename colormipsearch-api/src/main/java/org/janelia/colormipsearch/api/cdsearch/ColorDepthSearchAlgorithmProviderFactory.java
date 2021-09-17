package org.janelia.colormipsearch.api.cdsearch;

import java.util.function.BiPredicate;

import org.janelia.colormipsearch.api.imageprocessing.ColorTransformation;
import org.janelia.colormipsearch.api.imageprocessing.ImageArray;
import org.janelia.colormipsearch.api.imageprocessing.ImageProcessing;
import org.janelia.colormipsearch.api.imageprocessing.ImageTransformation;
import org.janelia.colormipsearch.api.imageprocessing.LImage;
import org.janelia.colormipsearch.api.imageprocessing.LImageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for a color depth search comparator.
 */
public class ColorDepthSearchAlgorithmProviderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ColorDepthSearchAlgorithmProviderFactory.class);

    /**
     * Create a color depth query searcher that calculates only positive scores.
     *
     * @param mirrorMask flag whether to use mirroring
     * @param targetThreshold data threshold
     * @param pixColorFluctuation z - gap tolerance - sometimes called pixel color fluctuation
     * @param xyShift - x-y translation for searching for a match
     * @return a color depth search search provider
     */
    public static ColorDepthSearchAlgorithmProvider<ColorMIPMatchScore> createPixMatchCDSAlgorithmProvider(
            boolean mirrorMask,
            int targetThreshold,
            double pixColorFluctuation,
            int xyShift) {
        LOG.info("Create mask comparator with mirrorQuery={}, dataThreshold={}, pixColorFluctuation={}, xyShift={}",
                mirrorMask, targetThreshold, pixColorFluctuation, xyShift);
        return new ColorDepthSearchAlgorithmProvider<ColorMIPMatchScore>() {
            ColorDepthSearchParams defaultCDSParams = new ColorDepthSearchParams()
                    .setParam("mirrorMask", mirrorMask)
                    .setParam("dataThreshold", targetThreshold)
                    .setParam("pixColorFluctuation", pixColorFluctuation)
                    .setParam("xyShift", xyShift);
            @Override
            public ColorDepthSearchParams getDefaultCDSParams() {
                return defaultCDSParams;
            }

            @Override
            public ColorDepthSearchAlgorithm<ColorMIPMatchScore> createColorDepthQuerySearchAlgorithm(ImageArray<?> queryImageArray,
                                                                                                      int queryThreshold,
                                                                                                      ColorDepthSearchParams cdsParams) {
                Double pixColorFluctuationParam = cdsParams.getDoubleParam("pixColorFluctuation", pixColorFluctuation);
                double zTolerance = pixColorFluctuationParam == null ? 0. : pixColorFluctuationParam / 100;
                return new PixelMatchColorDepthSearchAlgorithm(
                        queryImageArray,
                        queryThreshold,
                        cdsParams.getBoolParam("mirrorMask", mirrorMask),
                        null,
                        0,
                        false,
                        cdsParams.getIntParam("dataThreshold", targetThreshold),
                        zTolerance,
                        cdsParams.getIntParam("xyShift", xyShift));
            }
        };
    }

    public static ColorDepthSearchAlgorithmProvider<NegativeColorDepthMatchScore> createNegativeMatchCDSAlgorithmProvider(
            boolean mirrorMask,
            int negativeRadius,
            ImageArray<?> roiMaskImageArray) {
        if (negativeRadius <= 0) {
            throw new IllegalArgumentException("The value for negative radius must be a positive integer - current value is " + negativeRadius);
        }
        return new ColorDepthSearchAlgorithmProvider<NegativeColorDepthMatchScore>() {
            ColorDepthSearchParams defaultCDSParams = new ColorDepthSearchParams()
                    .setParam("mirrorMask", mirrorMask)
                    .setParam("negativeRadius", negativeRadius);

            @Override
            public ColorDepthSearchParams getDefaultCDSParams() {
                return defaultCDSParams;
            }

            @Override
            public ColorDepthSearchAlgorithm<NegativeColorDepthMatchScore> createColorDepthQuerySearchAlgorithm(ImageArray<?> queryImageArray,
                                                                                                                int queryThreshold,
                                                                                                                ColorDepthSearchParams cdsParams) {
                BiPredicate<Integer, Integer> isLabel = ImageTransformation.getLabelRegionCond(queryImageArray.getWidth());
                ImageTransformation clearLabels = ImageTransformation.clearRegion(isLabel);

                ImageProcessing negativeRadiusDilation = ImageProcessing.create(clearLabels)
                        .mask(queryThreshold)
                        .maxFilter(cdsParams.getIntParam("negativeRadius", negativeRadius));
                long startTime = System.currentTimeMillis();
                LImage roiMaskImage;
                if (roiMaskImageArray == null) {
                    roiMaskImage = null;
                } else {
                    roiMaskImage = LImageUtils.create(roiMaskImageArray).mapi(clearLabels);
                }
                LImage queryImage = LImageUtils.create(queryImageArray).mapi(clearLabels);

                LImage maskForRegionsWithTooMuchExpression = LImageUtils.combine2(
                        queryImage.mapi(ImageTransformation.maxFilter(60)),
                        queryImage.mapi(ImageTransformation.maxFilter(20)),
                        (p1, p2) -> {
                            return p2 != -16777216 && p2 != 0 ? -16777216 : p1;
                        } // mask pixels from the 60x image if they are present in the 20x image
                );
                GradientBasedNegativeScoreColorDepthSearchAlgorithm maskNegativeScoresCalculator = new GradientBasedNegativeScoreColorDepthSearchAlgorithm(
                        queryImage,
                        queryImage.map(ColorTransformation.toGray16WithNoGammaCorrection()).map(ColorTransformation.toSignalRegions(2)).reduce(),
                        maskForRegionsWithTooMuchExpression.map(ColorTransformation.toGray16WithNoGammaCorrection()).map(ColorTransformation.toSignalRegions(0)).reduce(),
                        roiMaskImage,
                        cdsParams.getIntParam("queryThreshold", queryThreshold),
                        cdsParams.getBoolParam("mirrorMask", mirrorMask),
                        clearLabels,
                        negativeRadiusDilation
                );

                LOG.debug("Created gradient area gap calculator for mask in {}ms", System.currentTimeMillis() - startTime);
                return maskNegativeScoresCalculator;
            }
        };
    }

    public static ColorDepthSearchAlgorithmProvider<ColorMIPMatchScore> createPixMatchWithNegativeScoreCDSAlgorithmProvider(
            boolean mirrorMask,
            int targetThreshold,
            double pixColorFluctuation,
            int xyShift,
            int negativeRadius,
            ImageArray<?> roiMaskImageArray) {
        if (negativeRadius <= 0) {
            throw new IllegalArgumentException("The value for negative radius must be a positive integer - current value is " + negativeRadius);
        }
        return new ColorDepthSearchAlgorithmProvider<ColorMIPMatchScore>() {
            ColorDepthSearchParams defaultCDSParams = new ColorDepthSearchParams()
                        .setParam("mirrorMask", mirrorMask)
                        .setParam("dataThreshold", targetThreshold)
                        .setParam("pixColorFluctuation", pixColorFluctuation)
                        .setParam("xyShift", xyShift)
                        .setParam("negativeRadius", negativeRadius);

            @Override
            public ColorDepthSearchParams getDefaultCDSParams() {
                return defaultCDSParams;
            }

            @Override
            public ColorDepthSearchAlgorithm<ColorMIPMatchScore> createColorDepthQuerySearchAlgorithm(ImageArray<?> queryImageArray,
                                                                                                      int queryThreshold,
                                                                                                      ColorDepthSearchParams cdsParams) {
                ColorDepthSearchAlgorithm<ColorMIPMatchScore> posScoreCDSAlg =
                        createPixMatchCDSAlgorithmProvider(mirrorMask, targetThreshold, pixColorFluctuation, xyShift)
                                .createColorDepthQuerySearchAlgorithm(queryImageArray, queryThreshold, cdsParams);
                ColorDepthSearchAlgorithm<NegativeColorDepthMatchScore> negScoreCDSAlg =
                        createNegativeMatchCDSAlgorithmProvider(mirrorMask, negativeRadius, roiMaskImageArray)
                                .createColorDepthQuerySearchAlgorithm(queryImageArray, queryThreshold, cdsParams);
                return new PixelMatchWithNegativeScoreColorDepthSearchAlgorithm(posScoreCDSAlg, negScoreCDSAlg);
            }
        };
    }

}
