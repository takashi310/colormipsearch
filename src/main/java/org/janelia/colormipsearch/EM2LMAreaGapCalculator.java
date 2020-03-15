package org.janelia.colormipsearch;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This can be used to adjust the score for an EM mask against an LM (segmented) library
 */
class EM2LMAreaGapCalculator {

    private static final Logger LOG = LoggerFactory.getLogger(EM2LMAreaGapCalculator.class);
    private static final int DEFAULT_COLOR_FLUX = 40;

    private final int maskThreshold;
    private final int negativeRadius;
    private final boolean mirrorMask;

    EM2LMAreaGapCalculator(int maskThreshold, int negativeRadius, boolean mirrorMask) {
        this.maskThreshold = maskThreshold;
        this.negativeRadius = negativeRadius;
        this.mirrorMask = mirrorMask;
    }

    ColorMIPSearchResult.AreaGap calculateAdjustedScore(MIPImage libraryMIP, MIPImage patternMIP, MIPImage libraryGradient) {
        if (libraryGradient == null) {
            LOG.trace("No gradient image provided for {}", libraryMIP);
            return null;
        } else {
            long startTimestamp = System.currentTimeMillis();
            try {
                LOG.trace("Calculate area gap {} mirror mask between {} - {} using {}", mirrorMask ? "with" : "without", libraryMIP, patternMIP, libraryGradient);
                long adjustmentForNormalImage = calculateScoreAdjustment(libraryMIP, patternMIP, libraryGradient, ImageOperations.PixelTransformation.identity());
                if (mirrorMask) {
                    long adjustmentForMirroredImage = calculateScoreAdjustment(libraryMIP, patternMIP, libraryGradient, ImageOperations.PixelTransformation.toMirror());
                    if (adjustmentForNormalImage <= adjustmentForMirroredImage) {
                        return new ColorMIPSearchResult.AreaGap(adjustmentForNormalImage, false);
                    } else {
                        return new ColorMIPSearchResult.AreaGap(adjustmentForMirroredImage, true);
                    }
                } else {
                    return new ColorMIPSearchResult.AreaGap(adjustmentForNormalImage, false);
                }
            } finally {
                LOG.debug("Finished calculating area gap {} mirror mask between {} - {} using {} in {}ms", mirrorMask ? "with" : "without", libraryMIP, patternMIP, libraryGradient, System.currentTimeMillis() - startTimestamp);
            }
        }
    }

    private long calculateScoreAdjustment(MIPImage libraryMIP, MIPImage pattern, MIPImage libraryGradient, Function<ImageOperations.LImage, ImageOperations.PixelTransformation> mipTransformation) {
        ImageOperations.ImageProcessing dilatedLibraryProcessing = ImageOperations.ImageProcessing.createFor(libraryMIP)
                .mask(maskThreshold)
                .maxFilter(negativeRadius);

        ImageOperations.ImageProcessing patternProcessing = ImageOperations.ImageProcessing.createFor(pattern);

        ImageOperations.ImageProcessing patternSignalProcessing = patternProcessing
                .toGray16()
                .toSignal()
                .compose(mipTransformation)
                ;

        ImageOperations.ImageProcessing scoreAdjustmentTransformer = ImageOperations.ImageProcessing.createFor(libraryGradient)
                .combineWith(patternSignalProcessing, (p1, p2) -> p1 * p2)
                ;

        return scoreAdjustmentTransformer
                .combineWith(patternProcessing, dilatedLibraryProcessing, (p, patternPix, dilatedPix) -> {
                    if (dilatedPix != -16777216) {
                        if (patternPix != -16777216) {
                            int pxGapSlice = calculateSliceGap(patternPix, dilatedPix);
                            if (DEFAULT_COLOR_FLUX <= pxGapSlice - DEFAULT_COLOR_FLUX) {
                                // negative score value
                                return pxGapSlice - DEFAULT_COLOR_FLUX;
                            }
                        }
                    }
                    return p;
                })
                .stream()
                .filter(p -> p > 3).mapToLong(p -> p)
                .reduce(0L, (p1, p2) -> p1 + p2)
                ;
    }

    private Operations.PixelTransformation scoreAdjustment(MIPImage pattern, MIPImage dilatedLibray) {
        return (x, y, c) -> {
            int dilatedPix = dilatedLibray.getPixel(x, y);
            if (dilatedPix != -16777216) {
                int patternPix = pattern.getPixel(x, y);
                if (patternPix != -16777216) {
                    int pxGapSlice = calculateSliceGap(patternPix, dilatedPix);
                    if (DEFAULT_COLOR_FLUX <= pxGapSlice - DEFAULT_COLOR_FLUX) {
                        // negative score value
                        return pxGapSlice - DEFAULT_COLOR_FLUX;
                    }
                }
            }
            return c;
        };
    }

    private int calculateSliceGap(int rgb1, int rgb2) {

        int max1stvalMASK = 0, max2ndvalMASK = 0, max1stvalDATA = 0, max2ndvalDATA = 0, maskslinumber = 0, dataslinumber = 0;
        String mask1stMaxColor = "Black", mask2ndMaxColor = "Black", data1stMaxColor = "Black", data2ndMaxColor = "Black";

        int red1 = (rgb1 >>> 16) & 0xff;
        int green1 = (rgb1 >>> 8) & 0xff;
        int blue1 = rgb1 & 0xff;

        int red2 = (rgb2 >>> 16) & 0xff;
        int green2 = (rgb2 >>> 8) & 0xff;
        int blue2 = rgb2 & 0xff;

        if (red1 >= green1 && red1 >= blue1) {
            max1stvalMASK = red1;
            mask1stMaxColor = "red";
            if (green1 >= blue1) {
                max2ndvalMASK = green1;
                mask2ndMaxColor = "green";
            } else {
                max2ndvalMASK = blue1;
                mask2ndMaxColor = "blue";
            }
        } else if (green1 >= red1 && green1 >= blue1) {
            max1stvalMASK = green1;
            mask1stMaxColor = "green";
            if (red1 >= blue1) {
                mask2ndMaxColor = "red";
                max2ndvalMASK = red1;
            } else {
                max2ndvalMASK = blue1;
                mask2ndMaxColor = "blue";
            }
        } else if (blue1 >= red1 && blue1 >= green1) {
            max1stvalMASK = blue1;
            mask1stMaxColor = "blue";
            if (red1 >= green1) {
                max2ndvalMASK = red1;
                mask2ndMaxColor = "red";
            } else {
                max2ndvalMASK = green1;
                mask2ndMaxColor = "green";
            }
        }

        if (red2 >= green2 && red2 >= blue2) {
            max1stvalDATA = red2;
            data1stMaxColor = "red";
            if (green2 >= blue2) {
                max2ndvalDATA = green2;
                data2ndMaxColor = "green";
            } else {
                max2ndvalDATA = blue2;
                data2ndMaxColor = "blue";
            }
        } else if (green2 >= red2 && green2 >= blue2) {
            max1stvalDATA = green2;
            data1stMaxColor = "green";
            if (red2 >= blue2) {
                max2ndvalDATA = red2;
                data2ndMaxColor = "red";
            } else {
                max2ndvalDATA = blue2;
                data2ndMaxColor = "blue";
            }
        } else if (blue2 >= red2 && blue2 >= green2) {
            max1stvalDATA = blue2;
            data1stMaxColor = "blue";
            if (red2 >= green2) {
                max2ndvalDATA = red2;
                data2ndMaxColor = "red";
            } else {
                max2ndvalDATA = green2;
                data2ndMaxColor = "green";
            }
        }

        double maskratio = (double) max2ndvalMASK / (double) max1stvalMASK;
        double dataratio = (double) max2ndvalDATA / (double) max1stvalDATA;
        maskslinumber = findSliceNumber(mask1stMaxColor, mask2ndMaxColor, maskratio);
        dataslinumber = findSliceNumber(data1stMaxColor, data2ndMaxColor, dataratio);

        if (dataslinumber == 0 || maskslinumber == 0) {
            return (int) dataslinumber;
        }

        int gapslicenum = Math.abs(maskslinumber - dataslinumber);
        return gapslicenum;
    }

    private int findSliceNumber(String maxColor, String secondMaxColor, double colorRatio) {
        switch (maxColor) {
            case "red": //cheking slice num 172-256
                if (secondMaxColor.equals("green")) { // 172-213
                    return findSliceNumberInLUT(171, 212, colorRatio);
                } else if (secondMaxColor.equals("blue"))//214-256
                    return findSliceNumberInLUT(213, 255, colorRatio);
                break;
            case "green":  // cheking slice num 87-171
                if (secondMaxColor.equals("red")) // 129-171
                    return findSliceNumberInLUT(128, 170, colorRatio);
                if (secondMaxColor.equals("blue")) // 87-128
                    return findSliceNumberInLUT(86, 127, colorRatio);
                break;
            case "blue":  // cheking slice num 1-86 = 0-85
                if (secondMaxColor.equals("red")) // 1-30
                    return findSliceNumberInLUT(0, 29, colorRatio);
                if (secondMaxColor.equals("green")) // 31-86
                    return findSliceNumberInLUT(30, 85, colorRatio);
                break;
        }
        return 0;
    }

    private int findSliceNumberInLUT(int lutStartRange, int lutEndRange, double colorRatio) {
        short[][] lut = {
                {127, 0, 255}, {125, 3, 255}, {124, 6, 255}, {122, 9, 255}, {121, 12, 255}, {120, 15, 255}, {119, 18, 255}, {118, 21, 255}, {116, 24, 255}, {115, 27, 255}, {114, 30, 255}, {113, 33, 255},
                {112, 36, 255}, {110, 39, 255}, {109, 42, 255}, {108, 45, 255}, {106, 48, 255}, {105, 51, 255}, {104, 54, 255}, {103, 57, 255}, {101, 60, 255}, {100, 63, 255}, {99, 66, 255}, {98, 69, 255},
                {96, 72, 255}, {95, 75, 255}, {94, 78, 255}, {93, 81, 255}, {92, 84, 255}, {90, 87, 255}, {89, 90, 255}, {87, 93, 255}, {86, 96, 255}, {84, 99, 255}, {83, 102, 255}, {81, 105, 255},
                {80, 108, 255}, {78, 111, 255}, {77, 114, 255}, {75, 117, 255}, {74, 120, 255}, {72, 123, 255}, {71, 126, 255}, {69, 129, 255}, {68, 132, 255}, {66, 135, 255}, {65, 138, 255}, {63, 141, 255},
                {62, 144, 255}, {60, 147, 255}, {59, 150, 255}, {57, 153, 255}, {56, 156, 255}, {54, 159, 255}, {53, 162, 255}, {51, 165, 255}, {50, 168, 255}, {48, 171, 255}, {47, 174, 255}, {45, 177, 255},
                {44, 180, 255}, {42, 183, 255}, {41, 186, 255}, {39, 189, 255}, {38, 192, 255}, {36, 195, 255}, {35, 198, 255}, {33, 201, 255}, {32, 204, 255}, {30, 207, 255}, {29, 210, 255}, {27, 213, 255},
                {26, 216, 255}, {24, 219, 255}, {23, 222, 255}, {21, 225, 255}, {20, 228, 255}, {18, 231, 255}, {16, 234, 255}, {14, 237, 255}, {12, 240, 255}, {9, 243, 255}, {6, 246, 255}, {3, 249, 255},
                {1, 252, 255}, {0, 254, 255}, {3, 255, 252}, {6, 255, 249}, {9, 255, 246}, {12, 255, 243}, {15, 255, 240}, {18, 255, 237}, {21, 255, 234}, {24, 255, 231}, {27, 255, 228}, {30, 255, 225},
                {33, 255, 222}, {36, 255, 219}, {39, 255, 216}, {42, 255, 213}, {45, 255, 210}, {48, 255, 207}, {51, 255, 204}, {54, 255, 201}, {57, 255, 198}, {60, 255, 195}, {63, 255, 192}, {66, 255, 189},
                {69, 255, 186}, {72, 255, 183}, {75, 255, 180}, {78, 255, 177}, {81, 255, 174}, {84, 255, 171}, {87, 255, 168}, {90, 255, 165}, {93, 255, 162}, {96, 255, 159}, {99, 255, 156}, {102, 255, 153},
                {105, 255, 150}, {108, 255, 147}, {111, 255, 144}, {114, 255, 141}, {117, 255, 138}, {120, 255, 135}, {123, 255, 132}, {126, 255, 129}, {129, 255, 126}, {132, 255, 123}, {135, 255, 120},
                {138, 255, 117}, {141, 255, 114}, {144, 255, 111}, {147, 255, 108}, {150, 255, 105}, {153, 255, 102}, {156, 255, 99}, {159, 255, 96}, {162, 255, 93}, {165, 255, 90}, {168, 255, 87}, {171, 255, 84},
                {174, 255, 81}, {177, 255, 78}, {180, 255, 75}, {183, 255, 72}, {186, 255, 69}, {189, 255, 66}, {192, 255, 63}, {195, 255, 60}, {198, 255, 57}, {201, 255, 54}, {204, 255, 51}, {207, 255, 48},
                {210, 255, 45}, {213, 255, 42}, {216, 255, 39}, {219, 255, 36}, {222, 255, 33}, {225, 255, 30}, {228, 255, 27}, {231, 255, 24}, {234, 255, 21}, {237, 255, 18}, {240, 255, 15}, {243, 255, 12},
                {246, 255, 9}, {249, 255, 6}, {252, 255, 3}, {254, 255, 0}, {255, 252, 3}, {255, 249, 6}, {255, 246, 9}, {255, 243, 12}, {255, 240, 15}, {255, 237, 18}, {255, 234, 21}, {255, 231, 24}, {255, 228, 27},
                {255, 225, 30}, {255, 222, 33}, {255, 219, 36}, {255, 216, 39}, {255, 213, 42}, {255, 210, 45}, {255, 207, 48}, {255, 204, 51}, {255, 201, 54}, {255, 198, 57}, {255, 195, 60}, {255, 192, 63},
                {255, 189, 66}, {255, 186, 69}, {255, 183, 72}, {255, 180, 75}, {255, 177, 78}, {255, 174, 81}, {255, 171, 84}, {255, 168, 87}, {255, 165, 90}, {255, 162, 93}, {255, 159, 96}, {255, 156, 99},
                {255, 153, 102}, {255, 150, 105}, {255, 147, 108}, {255, 144, 111}, {255, 141, 114}, {255, 138, 117}, {255, 135, 120}, {255, 132, 123}, {255, 129, 126}, {255, 126, 129}, {255, 123, 132},
                {255, 120, 135}, {255, 117, 138}, {255, 114, 141}, {255, 111, 144}, {255, 108, 147}, {255, 105, 150}, {255, 102, 153}, {255, 99, 156}, {255, 96, 159}, {255, 93, 162}, {255, 90, 165}, {255, 87, 168},
                {255, 84, 171}, {255, 81, 173}, {255, 78, 174}, {255, 75, 175}, {255, 72, 176}, {255, 69, 177}, {255, 66, 178}, {255, 63, 179}, {255, 60, 180}, {255, 57, 181}, {255, 54, 182}, {255, 51, 183},
                {255, 48, 184}, {255, 45, 185}, {255, 42, 186}, {255, 39, 187}, {255, 36, 188}, {255, 33, 189}, {255, 30, 190}, {255, 27, 191}, {255, 24, 192}, {255, 21, 193}, {255, 18, 194}, {255, 15, 195},
                {255, 12, 196}, {255, 9, 197}, {255, 6, 198}, {255, 3, 199}, {255, 0, 200}
        };

        int sliceNumber = 0;
        double mingapratio = 1000;
        for (int icolor = lutStartRange; icolor <= lutEndRange; icolor++) {

            short[] coloraray = lut[icolor];
            double lutRatio = 0;

            double colorR = coloraray[0];
            double colorG = coloraray[1];
            double colorB = coloraray[2];

            if (colorB > colorR && colorB > colorG) {
                if (colorR > colorG)
                    lutRatio = colorR / colorB;
                else if (colorG > colorR)
                    lutRatio = colorG / colorB;
            } else if (colorG > colorR && colorG > colorB) {
                if (colorR > colorB)
                    lutRatio = colorR / colorG;
                else if (colorB > colorR)
                    lutRatio = colorB / colorG;
            } else if (colorR > colorG && colorR > colorB) {
                if (colorG > colorB)
                    lutRatio = colorG / colorR;
                else if (colorB > colorG)
                    lutRatio = colorB / colorR;
            }

            if (lutRatio == colorRatio) {
                return icolor + 1;
            }

            double gapratio = Math.abs(colorRatio - lutRatio);

            if (gapratio < mingapratio) {
                mingapratio = gapratio;
                sliceNumber = icolor + 1;
            }
        }
        return sliceNumber;
    }

}
