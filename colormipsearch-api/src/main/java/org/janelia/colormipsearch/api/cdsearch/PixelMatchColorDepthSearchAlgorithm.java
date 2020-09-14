package org.janelia.colormipsearch.api.cdsearch;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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

    public PixelMatchColorDepthSearchAlgorithm(ImageArray query, int queryThreshold, boolean mirrorQuery,
                                               ImageArray negQuery, int negQueryThreshold,
                                               boolean mirrorNegQuery, int targetThreshold,
                                               double zTolerance, int xyshift) {
        super(query, queryThreshold, negQuery, negQueryThreshold, targetThreshold, zTolerance);
        // shifting
        targetMasksList = generateShiftedMasks(queryPositions, xyshift, query.getWidth(), query.getHeight());
        if (negQueryImage != null) {
            negTargetMasksList = generateShiftedMasks(negQueryPositions, xyshift, query.getWidth(), query.getHeight());
        } else {
            negTargetMasksList = null;
        }

        // mirroring
        if (mirrorQuery) {
            mirrorTargetMasksList = new int[1 + (xyshift / 2) * 8][];
            for (int i = 0; i < targetMasksList.length; i++)
                mirrorTargetMasksList[i] = mirrorMask(targetMasksList[i], query.getWidth());
        } else {
            mirrorTargetMasksList = null;
        }
        if (mirrorNegQuery && negQueryImage != null) {
            negMirrorTargetMasksList = new int[1 + (xyshift / 2) * 8][];
            for (int i = 0; i < negTargetMasksList.length; i++)
                negMirrorTargetMasksList[i] = mirrorMask(negTargetMasksList[i], query.getWidth());
        } else {
            negMirrorTargetMasksList = null;
        }
    }

    @Override
    public ColorMIPMatchScore calculateMatchingScore(@Nonnull ImageArray targetImageArray,
                                                     @Nullable ImageArray targetGradientImageArray,
                                                     @Nullable ImageArray targetZGapMaskImageArray) {
        int posi = 0;
        double posipersent = 0.0;
        int masksize = queryPositions.length;
        int negmasksize = negQueryPositions != null ? negQueryPositions.length : 0;

        for (int[] ints : targetMasksList) {
            int tmpposi = calculateScore(queryImage, queryPositions, targetImageArray, ints);
            if (tmpposi > posi) {
                posi = tmpposi;
                posipersent = (double) posi / (double) masksize;
            }
        }
        if (negTargetMasksList != null) {
            int nega = 0;
            double negapersent = 0.0;
            for (int[] ints : negTargetMasksList) {
                int tmpnega = calculateScore(negQueryImage, negQueryPositions, targetImageArray, ints);
                if (tmpnega > nega) {
                    nega = tmpnega;
                    negapersent = (double) nega / (double) negmasksize;
                }
            }
            posipersent -= negapersent;
            posi = (int) Math.round((double) posi - (double) nega * ((double) masksize / (double) negmasksize));
        }

        if (mirrorTargetMasksList != null) {
            int mirror_posi = 0;
            double mirror_posipersent = 0.0;
            for (int[] ints : mirrorTargetMasksList) {
                int tmpposi = calculateScore(queryImage, queryPositions, targetImageArray, ints);
                if (tmpposi > mirror_posi) {
                    mirror_posi = tmpposi;
                    mirror_posipersent = (double) mirror_posi / (double) masksize;
                }
            }
            if (negMirrorTargetMasksList != null) {
                int nega = 0;
                double negapersent = 0.0;
                for (int[] ints : negMirrorTargetMasksList) {
                    int tmpnega = calculateScore(negQueryImage, negQueryPositions, targetImageArray, ints);
                    if (tmpnega > nega) {
                        nega = tmpnega;
                        negapersent = (double) nega / (double) negmasksize;
                    }
                }
                mirror_posipersent -= negapersent;
                mirror_posi = (int) Math.round((double) mirror_posi - (double) nega * ((double) masksize / (double) negmasksize));
            }
            if (posipersent < mirror_posipersent) {
                posi = mirror_posi;
                posipersent = mirror_posipersent;
            }
        }

        return new ColorMIPMatchScore(posi, posipersent, null);
    }


    private int[][] generateShiftedMasks(int[] in, int xyshift, int imageWidth, int imageHeight) {
        int[][] out = new int[1 + (xyshift / 2) * 8][];

        out[0] = in.clone();
        int maskid = 1;
        for (int i = 2; i <= xyshift; i += 2) {
            for (int xx = -i; xx <= i; xx += i) {
                for (int yy = -i; yy <= i; yy += i) {
                    if (xx == 0 && yy == 0) continue;
                    out[maskid] = shiftMaskPosArray(in, xx, yy, imageWidth, imageHeight);
                    maskid++;
                }
            }
        }
        return out;
    }

    private int[] shiftMaskPosArray(int[] src, int xshift, int yshift, int imageWidth, int imageHeight) {
        List<Integer> pos = new ArrayList<>();
        int x, y;
        for (int i = 0; i < src.length; i++) {
            int val = src[i];
            x = (val % imageWidth) + xshift;
            y = val / imageWidth + yshift;
            if (x >= 0 && x < imageWidth && y >= 0 && y < imageHeight)
                pos.add(y * imageWidth + x);
            else
                pos.add(-1);
        }
        return pos.stream().mapToInt(i -> i).toArray();
    }

    private int[] mirrorMask(int[] in, int ypitch) {
        int[] out = in.clone();
        int masksize = in.length;
        int x;
        for (int j = 0; j < masksize; j++) {
            int val = in[j];
            x = val % ypitch;
            out[j] = val + (ypitch - 1) - 2 * x;
        }
        return out;
    }

    private int calculateScore(ImageArray src, int[] scrPositions, ImageArray tar, int[] targetPositions) {
        int masksize = scrPositions.length <= targetPositions.length ? scrPositions.length : targetPositions.length;
        int posi = 0;
        for (int masksig = 0; masksig < masksize; masksig++) {
            if (scrPositions[masksig] == -1 || targetPositions[masksig] == -1) continue;

            int pix1 = src.get(scrPositions[masksig]);
            int red1 = (pix1 >>> 16) & 0xff;
            int green1 = (pix1 >>> 8) & 0xff;
            int blue1 = pix1 & 0xff;

            int pix2 = tar.get(targetPositions[masksig]);
            int red2 = (pix2 >>> 16) & 0xff;
            int green2 = (pix2 >>> 8) & 0xff;
            int blue2 = pix2 & 0xff;

            if (red2 > targetThreshold || green2 > targetThreshold || blue2 > targetThreshold) {
                double pxGap = calculatePixelGap(red1, green1, blue1, red2, green2, blue2);
                if (pxGap <= zTolerance) {
                    posi++;
                }
            }
        }
        return posi;
    }

}
