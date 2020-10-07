package org.janelia.colormipsearch.api.cdsearch;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.janelia.colormipsearch.api.imageprocessing.ImageArray;

/**
 * PixelMatchColorDepthQuerySearchAlgorithm - implements the color depth mip comparison
 * using internal arrays containg the positions from the mask that are above the mask threshold
 * and the positions after applying the specified x-y shift and mirroring transformations.
 * The mask pixels are compared against the target pixels tht
 */
public class PixelMatchColorDepthSearchAlgorithm extends AbstractColorDepthSearchAlgorithm<ColorMIPMatchScore> {

    private final boolean mirrorQuery;
    private final boolean mirrorNegQuery;
    private final int[] allXYShifts;

    public PixelMatchColorDepthSearchAlgorithm(ImageArray queryImage, int queryThreshold, boolean mirrorQuery,
                                               ImageArray negQueryImage, int negQueryThreshold,
                                               boolean mirrorNegQuery, int targetThreshold,
                                               double zTolerance, int xyshift) {
        super(queryImage, queryThreshold, negQueryImage, negQueryThreshold, targetThreshold, zTolerance);
        this.mirrorQuery = mirrorQuery;
        this.mirrorNegQuery = mirrorNegQuery;
        this.allXYShifts = generateAllXYShifts(xyshift);
    }

    private int[] generateAllXYShifts(int xyshift) {
        int nshifts = 1 + (xyshift / 2) * 8;
        int[] xyShifts = new int[2 * nshifts];
        if (nshifts > 1) {
            int index = 0;
            for (int i = 2; i <= xyshift; i += 2) {
                for (int xx = -i; xx <= i; xx += i) {
                    for (int yy = -i; yy <= i; yy += i) {
                        xyShifts[index++] = xx;
                        xyShifts[index++] = yy;
                    }
                }
            }
        } else {
            xyShifts[0] = 0;
            xyShifts[1] = 0;
        }
        return xyShifts;
    }

    @Override
    public Set<String> getRequiredTargetVariantTypes() {
        return Collections.emptySet();
    }

    @Override
    public ColorMIPMatchScore calculateMatchingScore(@Nonnull ImageArray targetImageArray,
                                                     Map<String, Supplier<ImageArray>> variantTypeSuppliers) {
        int maxMatchingPixels = 0;
        int querySize = querySize();
        if (querySize == 0) {
            return new ColorMIPMatchScore(0, 0, null);
        }
        Function<Integer, Integer> mirrorTransformation = mirror(queryImage.getWidth());
        int xyShiftsMaxScore = calculateMaxScoreForAllTransformations(
                queryImage,
                this::streamQueryPixelPositions,
                streamXYShiftCoordTransformations(queryImage.getWidth(), queryImage.getHeight()),
                targetImageArray
        );
        if (xyShiftsMaxScore > maxMatchingPixels) {
            maxMatchingPixels = xyShiftsMaxScore;
        }
        if (mirrorQuery) {
            int mirroredXYShiftsMaxScore = calculateMaxScoreForAllTransformations(
                    queryImage,
                    this::streamQueryPixelPositions,
                    streamXYShiftCoordTransformations(queryImage.getWidth(), queryImage.getHeight())
                            .map(t -> t.andThen(mirrorTransformation)),
                    targetImageArray
            );
            if (mirroredXYShiftsMaxScore > maxMatchingPixels) {
                maxMatchingPixels = mirroredXYShiftsMaxScore;
            }
        }
        double maxMatchingPixelsRatio = (double)maxMatchingPixels / (double)querySize;
        int negQuerySize = negQuerySize();
        if (negQuerySize > 0) {
            int negativeMaxMatchingPixels = 0;
            int xyShiftsNegQueryMaxScore = calculateMaxScoreForAllTransformations(
                    negQueryImage,
                    this::streamNegQueryPixelPositions,
                    streamXYShiftCoordTransformations(negQueryImage.getWidth(), negQueryImage.getHeight()),
                    targetImageArray
            );
            if (xyShiftsNegQueryMaxScore > negativeMaxMatchingPixels) {
                negativeMaxMatchingPixels = xyShiftsNegQueryMaxScore;
            }
            if (mirrorNegQuery) {
                int mirroredXYShiftsNegQueryMaxScore = calculateMaxScoreForAllTransformations(
                        negQueryImage,
                        this::streamNegQueryPixelPositions,
                        streamXYShiftCoordTransformations(negQueryImage.getWidth(), negQueryImage.getHeight())
                            .map(t -> t.andThen(mirrorTransformation)),
                        targetImageArray
                );
                if (mirroredXYShiftsNegQueryMaxScore > negativeMaxMatchingPixels) {
                    negativeMaxMatchingPixels = mirroredXYShiftsNegQueryMaxScore;
                }
            }
            // reduce the matching pixels by the size of the negative match
            maxMatchingPixels = (int) Math.round((double)maxMatchingPixels - (double)negativeMaxMatchingPixels * querySize / (double)negQuerySize);
            maxMatchingPixelsRatio -= (double)negativeMaxMatchingPixels / (double)negQuerySize;
        }
        return new ColorMIPMatchScore(maxMatchingPixels, maxMatchingPixelsRatio, null);
    }

    private Function<Integer, Integer> shiftPos(int xshift, int yshift, int imageWidth, int imageHeight) {
        return (pos) -> {
            int x = (pos % imageWidth) + xshift;
            int y = pos / imageWidth + yshift;
            if (x >= 0 && x < imageWidth && y >= 0 && y < imageHeight) {
                return y * imageWidth + x;
            }
            return -1;
        };
    }

    private Function<Integer, Integer> mirror(int imageWidth) {
        return pos -> {
            int x = pos % imageWidth;
            return pos + (imageWidth - 1) - 2 * x;
        };
    }

    private int getPixel(ImageArray imageArray, int pixelIndex, Function<Integer, Integer> coordTransform) {
        int actualPixelIndex = coordTransform.apply(pixelIndex);
        if (actualPixelIndex != -1) {
            return imageArray.get(actualPixelIndex);
        } else {
            return 0;
        }
    }

    private Stream<Function<Integer, Integer>> streamXYShiftCoordTransformations(int width, int height) {
        return IntStream.range(0, allXYShifts.length / 2)
                .mapToObj(xyShiftIndex -> {
                    int xshift = allXYShifts[2 * xyShiftIndex];
                    int yshift = allXYShifts[2 * xyShiftIndex + 1];
                    return shiftPos(xshift, yshift, width, height);
                });
    }

    private int calculateMaxScoreForAllTransformations(ImageArray srcImageArray,
                                                       Supplier<IntStream> pixelCoordSupplier,
                                                       Stream<Function<Integer, Integer>> pixelCoordTransformations,
                                                       ImageArray targetImageArray) {
        return pixelCoordTransformations
                .map(pixelCoordTransform -> calculateScore(srcImageArray, pixelCoordSupplier.get(), targetImageArray, pixelCoordTransform))
                .max(Integer::compareTo)
                .orElse(0);
    }

    private int calculateScore(ImageArray srcImage, IntStream scrPositions, ImageArray targetImage, Function<Integer, Integer> targetPosTransform) {
        return scrPositions
                .map(srcPos -> {
                    int targetPos = targetPosTransform.apply(srcPos);
                    if (targetPos == -1) {
                        return 0;
                    } else {
                        int targetPix = targetImage.get(targetPos);
                        int red2 = (targetPix >>> 16) & 0xff;
                        int green2 = (targetPix >>> 8) & 0xff;
                        int blue2 = targetPix & 0xff;
                        if (red2 > targetThreshold || green2 > targetThreshold || blue2 > targetThreshold) {
                            int srcPixel = srcImage.get(srcPos);
                            int red1 = (srcPixel >>> 16) & 0xff;
                            int green1 = (srcPixel >>> 8) & 0xff;
                            int blue1 = srcPixel & 0xff;
                            double pxGap = calculatePixelGap(red1, green1, blue1, red2, green2, blue2);
                            if (pxGap <= zTolerance) {
                                return 1;
                            } else {
                                return 0;
                            }
                        } else {
                            return 0;
                        }

                    }
                })
                .sum();
    }

}
