package org.janelia.colormipsearch.api.cdsearch;

import java.util.ArrayList;
import java.util.List;

import org.janelia.colormipsearch.api.imageprocessing.ImageArray;

/**
 * ArrayColorMIPMaskCompare - implements the color depth mip comparison
 * using internal arrays containg the positions from the mask that are above the mask threshold
 * and the positions after applying the specified x-y shift and mirroring transformations.
 */
public class ArrayColorMIPMaskCompare extends ColorMIPMaskCompare {

    private final int[][] targetMasksList;
    private final int[][] mirrorTargetMasksList;
    private final int[][] negTargetMasksList;
    private final int[][] negMirrorTargetMasksList;

    public ArrayColorMIPMaskCompare(ImageArray query, int maskThreshold, boolean mirrorMask,
                                    ImageArray negquery, int negMaskThreshold,
                                    boolean mirrorNegMask, int searchThreshold,
                                    double zTolerance, int xyshift) {
        super(query, maskThreshold, negquery, negMaskThreshold, searchThreshold, zTolerance);
        // shifting
        targetMasksList = generateShiftedMasks(maskPositions, xyshift, query.getWidth(), query.getHeight());
        if (negQueryImage != null) {
            negTargetMasksList = generateShiftedMasks(negMaskPositions, xyshift, query.getWidth(), query.getHeight());
        } else {
            negTargetMasksList = null;
        }

        // mirroring
        if (mirrorMask) {
            mirrorTargetMasksList = new int[1 + (xyshift / 2) * 8][];
            for (int i = 0; i < targetMasksList.length; i++)
                mirrorTargetMasksList[i] = mirrorMask(targetMasksList[i], query.getWidth());
        } else {
            mirrorTargetMasksList = null;
        }
        if (mirrorNegMask && negQueryImage != null) {
            negMirrorTargetMasksList = new int[1 + (xyshift / 2) * 8][];
            for (int i = 0; i < negTargetMasksList.length; i++)
                negMirrorTargetMasksList[i] = mirrorMask(negTargetMasksList[i], query.getWidth());
        } else {
            negMirrorTargetMasksList = null;
        }

        int maskpos_st = query.getWidth() * query.getHeight();
        int maskpos_ed = 0;
        for (int i = 0; i < targetMasksList.length; i++) {
            if (targetMasksList[i][0] < maskpos_st) maskpos_st = targetMasksList[i][0];
            if (targetMasksList[i][targetMasksList[i].length-1] > maskpos_ed) maskpos_ed = targetMasksList[i][targetMasksList[i].length-1];
        }
        if (mirrorMask) {
            for (int i = 0; i < mirrorTargetMasksList.length; i++) {
                if (mirrorTargetMasksList[i][0] < maskpos_st) maskpos_st = mirrorTargetMasksList[i][0];
                if (mirrorTargetMasksList[i][mirrorTargetMasksList[i].length-1] > maskpos_ed) maskpos_ed = mirrorTargetMasksList[i][mirrorTargetMasksList[i].length-1];
            }
        }
        if (negQueryImage != null) {
            for (int i = 0; i < negTargetMasksList.length; i++) {
                if (negTargetMasksList[i][0] < maskpos_st) maskpos_st = negTargetMasksList[i][0];
                if (negTargetMasksList[i][negTargetMasksList[i].length-1] > maskpos_ed) maskpos_ed = negTargetMasksList[i][negTargetMasksList[i].length-1];
            }
            if (mirrorNegMask) {
                for (int i = 0; i < negMirrorTargetMasksList.length; i++) {
                    if (negMirrorTargetMasksList[i][0] < maskpos_st) maskpos_st = negMirrorTargetMasksList[i][0];
                    if (negMirrorTargetMasksList[i][negMirrorTargetMasksList[i].length-1] > maskpos_ed) maskpos_ed = negMirrorTargetMasksList[i][negMirrorTargetMasksList[i].length-1];
                }
            }
        }
        setMaskStartPosition(maskpos_st);
        setMaskEndPosition(maskpos_ed);
    }

    @Override
    public ColorMIPCompareOutput runSearch(ImageArray targetImage) {
        int posi = 0;
        double posipersent = 0.0;
        int masksize = maskPositions.length;
        int negmasksize = negMaskPositions != null ? negMaskPositions.length : 0;

        for (int[] ints : targetMasksList) {
            int tmpposi = calculateScore(queryImage, maskPositions, targetImage, ints);
            if (tmpposi > posi) {
                posi = tmpposi;
                posipersent = (double) posi / (double) masksize;
            }
        }
        if (negTargetMasksList != null) {
            int nega = 0;
            double negapersent = 0.0;
            for (int[] ints : negTargetMasksList) {
                int tmpnega = calculateScore(negQueryImage, negMaskPositions, targetImage, ints);
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
                int tmpposi = calculateScore(queryImage, maskPositions, targetImage, ints);
                if (tmpposi > mirror_posi) {
                    mirror_posi = tmpposi;
                    mirror_posipersent = (double) mirror_posi / (double) masksize;
                }
            }
            if (negMirrorTargetMasksList != null) {
                int nega = 0;
                double negapersent = 0.0;
                for (int[] ints : negMirrorTargetMasksList) {
                    int tmpnega = calculateScore(negQueryImage, negMaskPositions, targetImage, ints);
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

        return new ColorMIPCompareOutput(posi, posipersent);
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

        byte[] srcarr = (byte[])src.getPixels();
        byte[] tararr = (byte[])tar.getPixels();
        for (int masksig = 0; masksig < masksize; masksig++) {
            if (scrPositions[masksig] == -1 || targetPositions[masksig] == -1) continue;

            int p = scrPositions[masksig]*3;
            int red1 = srcarr[p] & 0xff;
            int green1 = srcarr[p+1] & 0xff;
            int blue1 = srcarr[p+2] & 0xff;

            p = targetPositions[masksig]*3;
            int red2 = tararr[p] & 0xff;
            int green2 = tararr[p+1] & 0xff;
            int blue2 = tararr[p+2] & 0xff;

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
