package org.janelia.colormipsearch.api;

import java.util.function.Function;

import org.janelia.colormipsearch.api.imageprocessing.ImageArray;

public class FColorMIPMaskCompare extends ColorMIPMaskCompare {

    private final boolean mirrorMask;
    private final boolean mirrorNegMask;
    private final int[] allXYShifts;

    // Advanced Search
    public FColorMIPMaskCompare(ImageArray query, int maskThreshold, boolean mirrorMask,
                                ImageArray negquery, int negMaskThreshold,
                                boolean mirrorNegMask, int searchThreshold,
                                double zTolerance, int xyshift) {
        super(query, maskThreshold, negquery, negMaskThreshold, searchThreshold, zTolerance);
        this.mirrorMask = mirrorMask;
        this.mirrorNegMask = mirrorNegMask;
        this.allXYShifts = generateAllXYShifts(xyshift);
    }

    public ColorMIPCompareOutput runSearch(ImageArray targetImage) {
        int posi = 0;
        double posipersent = 0.0;
        int masksize = maskPositions.length;
        int negmasksize = negQueryImage != null ? negMaskPositions.length : 0;

        Function<Integer, Integer> mirrorTransformation = mirror(queryImage.width);
        for (int xyShiftIndex = 0;  xyShiftIndex < allXYShifts.length; xyShiftIndex += 2) {
            int xshift = allXYShifts[xyShiftIndex];
            int yshift = allXYShifts[xyShiftIndex + 1];
            int tmpposi;
            Function<Integer, Integer> xyShiftTransform = shiftPos(xshift, yshift, queryImage.width, queryImage.height);
            tmpposi = calculateScore(queryImage, maskPositions, targetImage, xyShiftTransform);
            if (tmpposi > posi) {
                posi = tmpposi;
                posipersent = (double) posi / (double) masksize;
            }
            if (mirrorMask) {
                tmpposi = calculateScore(queryImage, maskPositions, targetImage, xyShiftTransform.andThen(mirrorTransformation));
                if (tmpposi > posi) {
                    posi = tmpposi;
                    posipersent = (double) posi / (double) masksize;
                }
            }
        }
        if (negmasksize > 0) {
            int nega = 0;
            double negapersent = 0.0;
            for (int xyShiftIndex = 0;  xyShiftIndex < allXYShifts.length; xyShiftIndex += 2) {
                int xshift = allXYShifts[xyShiftIndex];
                int yshift = allXYShifts[xyShiftIndex + 1];
                int tmpnega;
                Function<Integer, Integer> negXYShiftTransform = shiftPos(xshift, yshift, negQueryImage.width, negQueryImage.height);
                tmpnega = calculateScore(negQueryImage, negMaskPositions, targetImage, negXYShiftTransform);
                if (tmpnega > nega) {
                    nega = tmpnega;
                    negapersent = (double) nega / (double) negmasksize;
                }
                if (mirrorNegMask) {
                    tmpnega = calculateScore(negQueryImage, negMaskPositions, targetImage, negXYShiftTransform.andThen(mirrorTransformation));
                    if (tmpnega > nega) {
                        nega = tmpnega;
                        negapersent = (double) nega / (double) negmasksize;
                    }
                }
            }
            posipersent -= negapersent;
            posi = (int) Math.round((double) posi - (double) nega * ((double) masksize / (double) negmasksize));
        }

        return new ColorMIPCompareOutput(posi, posipersent);
    }

    private int[] generateAllXYShifts(int xyshift) {
        int initialCapacity = 1 + (xyshift / 2) * 8;
        int[] xyShifts = new int[2 * initialCapacity];
        if (initialCapacity > 1) {
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
            xyShifts[1] = 1;
        }
        return xyShifts;
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

    private int calculateScore(ImageArray src, int[] scrPositions, ImageArray tar, Function<Integer, Integer> targetTransformation) {
        int posi = 0;
        for (int srcPos : scrPositions) {
            int targetPos = targetTransformation.apply(srcPos);
            if (targetPos == -1) continue;

            int pix1 = src.get(srcPos);
            int red1 = (pix1 >>> 16) & 0xff;
            int green1 = (pix1 >>> 8) & 0xff;
            int blue1 = pix1 & 0xff;

            int pix2 = tar.get(targetPos);
            int red2 = (pix2 >>> 16) & 0xff;
            int green2 = (pix2 >>> 8) & 0xff;
            int blue2 = pix2 & 0xff;

            if (red2 > searchThreshold || green2 > searchThreshold || blue2 > searchThreshold) {
                double pxGap = calculatePixelGap(red1, green1, blue1, red2, green2, blue2);
                if (pxGap <= zTolerance) {
                    posi++;
                }
            }
        }
        return posi;
    }

}
