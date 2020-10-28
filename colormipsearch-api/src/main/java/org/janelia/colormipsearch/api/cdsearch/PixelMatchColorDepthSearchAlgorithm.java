package org.janelia.colormipsearch.api.cdsearch;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import org.janelia.colormipsearch.api.imageprocessing.ImageArray;

/**
 * PixelMatchColorDepthQuerySearchAlgorithm - implements the color depth mip comparison
 * using internal arrays containg the positions from the mask that are above the mask threshold
 * and the positions after applying the specified x-y shift and mirroring transformations.
 * The mask pixels are compared against the target pixels tht
 */
public class PixelMatchColorDepthSearchAlgorithm extends AbstractColorDepthSearchAlgorithm<ColorMIPMatchScore> {

    private final int[][] targetMasksList;
    private final int[][] mirrorTargetMasksList;
    private final int[][] negTargetMasksList;
    private final int[][] negMirrorTargetMasksList;
    private final int queryFirstPixelIndex;
    private final int queryLastPixelIndex;

    public PixelMatchColorDepthSearchAlgorithm(ImageArray<?> queryImage, int queryThreshold, boolean mirrorQuery,
                                               ImageArray<?> negQueryImage, int negQueryThreshold,
                                               boolean mirrorNegQuery, int targetThreshold,
                                               double zTolerance, int xyshift) {
        super(queryImage, queryThreshold, negQueryImage, negQueryThreshold, targetThreshold, zTolerance);
        // shifting
        targetMasksList = generateShiftedMasks(queryPixelPositions(), xyshift, queryImage.getWidth(), queryImage.getHeight());
        if (negQueryImage != null) {
            negTargetMasksList = generateShiftedMasks(negQueryPixelPositions(), xyshift, queryImage.getWidth(), queryImage.getHeight());
        } else {
            negTargetMasksList = null;
        }
        // mirroring
        if (mirrorQuery) {
            mirrorTargetMasksList = new int[1 + (xyshift / 2) * 8][];
            for (int i = 0; i < targetMasksList.length; i++)
                mirrorTargetMasksList[i] = mirrorMask(targetMasksList[i], queryImage.getWidth());
        } else {
            mirrorTargetMasksList = null;
        }
        // negative query mirroring
        if (mirrorNegQuery && negQueryImage != null) {
            negMirrorTargetMasksList = new int[1 + (xyshift / 2) * 8][];
            for (int i = 0; i < negTargetMasksList.length; i++)
                negMirrorTargetMasksList[i] = mirrorMask(negTargetMasksList[i], negQueryImage.getWidth());
        } else {
            negMirrorTargetMasksList = null;
        }
        // set strip boundaries
        int firstPixel = super.getQueryFirstPixelIndex();
        int lastPixel = super.getQueryLastPixelIndex();
        for (int i = 0; i < targetMasksList.length; i++) {
            if (targetMasksList[i][0] < firstPixel) firstPixel = targetMasksList[i][0];
            if (targetMasksList[i][targetMasksList[i].length-1] > lastPixel) lastPixel = targetMasksList[i][targetMasksList[i].length-1];
        }
        if (mirrorQuery) {
            for (int i = 0; i < mirrorTargetMasksList.length; i++) {
                if (mirrorTargetMasksList[i][0] < firstPixel) firstPixel = mirrorTargetMasksList[i][0];
                if (mirrorTargetMasksList[i][mirrorTargetMasksList[i].length-1] > lastPixel) lastPixel = mirrorTargetMasksList[i][mirrorTargetMasksList[i].length-1];
            }
        }
        if (negQueryImage != null) {
            for (int i = 0; i < negTargetMasksList.length; i++) {
                if (negTargetMasksList[i][0] < firstPixel) firstPixel = negTargetMasksList[i][0];
                if (negTargetMasksList[i][negTargetMasksList[i].length-1] > lastPixel) lastPixel = negTargetMasksList[i][negTargetMasksList[i].length-1];
            }
            if (mirrorNegQuery) {
                for (int i = 0; i < negMirrorTargetMasksList.length; i++) {
                    if (negMirrorTargetMasksList[i][0] < firstPixel) firstPixel = negMirrorTargetMasksList[i][0];
                    if (negMirrorTargetMasksList[i][negMirrorTargetMasksList[i].length-1] > lastPixel) lastPixel = negMirrorTargetMasksList[i][negMirrorTargetMasksList[i].length-1];
                }
            }
        }
        queryFirstPixelIndex = firstPixel;
        queryLastPixelIndex = lastPixel;
    }

    @Override
    public int getQueryFirstPixelIndex() {
        return queryFirstPixelIndex;
    }

    @Override
    public int getQueryLastPixelIndex() {
        return queryLastPixelIndex;
    }

    private int[][] generateShiftedMasks(int[] pixelCoords, int xyshift, int imageWidth, int imageHeight) {
        int nshifts = 1 + (xyshift / 2) * 8;
        int[][] out = new int[nshifts][];
        if (nshifts > 1) {
            int maskid = 0;
            for (int i = 2; i <= xyshift; i += 2) {
                for (int xx = -i; xx <= i; xx += i) {
                    for (int yy = -i; yy <= i; yy += i) {
                        out[maskid] = shiftMaskPosArray(pixelCoords, xx, yy, imageWidth, imageHeight);
                        maskid++;
                    }
                }
            }
        } else {
            out[0] = pixelCoords;
        }
        return out;
    }

    private int[] shiftMaskPosArray(int[] pixelCoords, int xshift, int yshift, int imageWidth, int imageHeight) {
        int[] shiftedCoords = new int[pixelCoords.length];
        for (int i = 0; i < pixelCoords.length; i++) {
            int pixelCoord = pixelCoords[i];
            int x = (pixelCoord % imageWidth) + xshift;
            int y = pixelCoord / imageWidth + yshift;
            if (x >= 0 && x < imageWidth && y >= 0 && y < imageHeight)
                shiftedCoords[i] = y * imageWidth + x;
            else
                shiftedCoords[i] = -1;
        }
        return shiftedCoords;
    }

    private int[] mirrorMask(int[] pixelCoords, int ypitch) {
        int[] mirroredCoords = new int[pixelCoords.length];
        for (int i = 0; i < pixelCoords.length; i++) {
            int pixelCoord = pixelCoords[i];
            if (pixelCoord == -1) {
                mirroredCoords[i] = -1;
            } else {
                int x = pixelCoord % ypitch;
                mirroredCoords[i] = pixelCoord + (ypitch - 1) - 2 * x;
            }
        }
        return mirroredCoords;
    }

    @Override
    public Set<String> getRequiredTargetVariantTypes() {
        return Collections.emptySet();
    }

    @Override
    public ColorMIPMatchScore calculateMatchingScore(@Nonnull ImageArray<?> targetImageArray,
                                                     Map<String, Supplier<ImageArray<?>>> variantTypeSuppliers) {
        int maxMatchingPixels = 0;
        int querySize = querySize();
        if (querySize == 0) {
            return new ColorMIPMatchScore(0, 0, null);
        }
        int xyShiftsMaxScore = calculateMaxScoreForAllTargetTransformations(
                queryImage,
                queryPixelPositions(),
                targetImageArray,
                targetMasksList);
        if (xyShiftsMaxScore > maxMatchingPixels) {
            maxMatchingPixels = xyShiftsMaxScore;
        }
        if (mirrorTargetMasksList != null) {
            int mirroredXYShiftsMaxScore = calculateMaxScoreForAllTargetTransformations(
                    queryImage,
                    queryPixelPositions(),
                    targetImageArray,
                    mirrorTargetMasksList
            );
            if (mirroredXYShiftsMaxScore > maxMatchingPixels) {
                maxMatchingPixels = mirroredXYShiftsMaxScore;
            }
        }
        double maxMatchingPixelsRatio = (double)maxMatchingPixels / (double)querySize;
        int negQuerySize = negQuerySize();
        if (negQuerySize > 0) {
            int negativeMaxMatchingPixels = 0;
            int xyShiftsNegQueryMaxScore = calculateMaxScoreForAllTargetTransformations(
                    negQueryImage,
                    queryPixelPositions(),
                    targetImageArray,
                    negTargetMasksList
            );
            if (xyShiftsNegQueryMaxScore > negativeMaxMatchingPixels) {
                negativeMaxMatchingPixels = xyShiftsNegQueryMaxScore;
            }
            if (negMirrorTargetMasksList != null) {
                int mirroredXYShiftsNegQueryMaxScore = calculateMaxScoreForAllTargetTransformations(
                        negQueryImage,
                        queryPixelPositions(),
                        targetImageArray,
                        negMirrorTargetMasksList
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

    private int calculateMaxScoreForAllTargetTransformations(ImageArray<?> srcImageArray,
                                                             int[] srcPixelCoord,
                                                             ImageArray<?> targetImageArray,
                                                             int[][] targetPixelCoordSupplier) {
        int maxScore = 0;
        for (int[] targetPixelCoord : targetPixelCoordSupplier) {
            int score = calculateScore(srcImageArray, srcPixelCoord, targetImageArray, targetPixelCoord);
            if (score > maxScore) {
                maxScore = score;
            }
        }
        return maxScore;
    }

    private int calculateScore(ImageArray<?> srcImage,
                               int[] srcPositions,
                               ImageArray<?> targetImage,
                               int[] targetPositions) {
        int size = Math.min(srcPositions.length, targetPositions.length);
        int score = 0;
        for (int i = 0; i < size; i++) {
            int srcPos = srcPositions[i];
            int targetPos = targetPositions[i];
            if (targetPos == -1 || srcPos == -1) {
                continue;
            }
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
                    score++;
                }
            }
        }
        return score;
    }

}
