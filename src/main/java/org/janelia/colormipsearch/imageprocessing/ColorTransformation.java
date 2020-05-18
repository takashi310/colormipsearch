package org.janelia.colormipsearch.imageprocessing;

import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class ColorTransformation implements BiFunction<ImageType, Integer, Integer> {
    final Function<ImageType, ImageType> pixelTypeChange;

    ColorTransformation(Function<ImageType, ImageType> pixelTypeChange) {
        this.pixelTypeChange = pixelTypeChange;
    }

    private static int maskGray(int val, int threshold) {
        return val <= threshold ? 0 : val;
    }

    private static int grayToBinary8(int val, int threshold) {
        return val <= threshold ? 0 : 255;
    }

    private static int grayToBinary16(int val, int threshold) {
        return val <= threshold ? 0 : 65535;
    }

    private static int maskRGB(int val, int threshold) {
        int r = (val >> 16) & 0xFF;
        int g = (val >> 8) & 0xFF;
        int b = (val & 0xFF);

        if (val != -16777216 && r <= threshold && g <= threshold && b <= threshold)
            return -16777216; // alpha mask
        else
            return val;
    }

    private static int rgbToGrayNoGammaCorrection(int rgb, float maxGrayValue) {
        if (rgb == -16777216) {
            return 0;
        } else {
            int r = (rgb >> 16) & 0xFF;
            int g = (rgb >> 8) & 0xFF;
            int b = (rgb & 0xFF);

            double rw = 1 / 3.;
            double gw = 1 / 3.;
            double bw = 1 / 3.;

            return (int) ((maxGrayValue / 255) * (r*rw + g*gw + b*bw + 0.5));
        }
    }

    private static int rgbToGrayWithGammaCorrection(int rgb, float maxGrayValue) {
        if (rgb == -16777216) {
            return 0;
        } else {
            int r = (rgb >> 16) & 0xFF;
            int g = (rgb >> 8) & 0xFF;
            int b = (rgb & 0xFF);

            // Normalize and gamma correct:
            float rr = (float) Math.pow(r / 255., 2.2);
            float gg = (float) Math.pow(g / 255., 2.2);
            float bb = (float) Math.pow(b / 255., 2.2);

            // Calculate luminance:
            float lum = 0.2126f * rr + 0.7152f * gg + 0.0722f * bb;
            // Gamma comp and rescale to byte range:
            return (int) (maxGrayValue * Math.pow(lum, 1.0 / 2.2));
        }
    }

    private static int scaleGray(int gray, float oldMax, float newMax) {
        return (int) (gray / oldMax * newMax);
    }

    static ColorTransformation toGray8(boolean withGammaCorrection) {
        return new ColorTransformation(pt -> ImageType.GRAY8) {
            @Override
            public Integer apply(ImageType pt, Integer pv) {
                switch (pt) {
                    case RGB:
                        return withGammaCorrection
                                ? ColorTransformation.rgbToGrayWithGammaCorrection(pv, 255)
                                : ColorTransformation.rgbToGrayNoGammaCorrection(pv, 255);
                    case GRAY8:
                        return pv;
                    case GRAY16:
                        return scaleGray(pv, 65535, 255);
                }
                throw new IllegalStateException("Cannot convert " + pt + " to gray8");
            }
        };
    }

    public static ColorTransformation toGray16() {
        return new ColorTransformation(pt -> ImageType.GRAY16) {
            @Override
            public Integer apply(ImageType pt, Integer pv) {
                switch (pt) {
                    case RGB:
                        return ColorTransformation.rgbToGrayNoGammaCorrection(pv, 65535);
                    case GRAY8:
                        return scaleGray(pv, 255, 65535);
                    case GRAY16:
                        return pv;
                }
                throw new IllegalStateException("Cannot convert " + pt + " to gray16");
            }
        };
    }

    static ColorTransformation mask(int threshold) {
        return new ColorTransformation(pt -> pt) {
            @Override
            public Integer apply(ImageType pt, Integer pv) {
                switch (pt) {
                    case RGB:
                        return maskRGB(pv, threshold);
                    case GRAY8:
                    case GRAY16:
                        return maskGray(pv, threshold);
                }
                throw new IllegalStateException("Cannot mask image type " + pt);
            }
        };
    }

    static ColorTransformation toBinary16(int threshold) {
        return ColorTransformation.toGray16().thenApplyColorTransformation(pv -> ColorTransformation.grayToBinary16(pv, threshold));
    }

    static ColorTransformation toBinary8(int threshold) {
        return ColorTransformation.toGray8(false).thenApplyColorTransformation(pv -> ColorTransformation.grayToBinary8(pv, threshold));
    }

    public static ColorTransformation toSignalRegions() {
        return new ColorTransformation(pt -> pt) {
            @Override
            public Integer apply(ImageType pt, Integer pv) {
                switch (pt) {
                    case RGB:
                        int r = ((pv >> 16) & 0xFF) > 0 ? 1 : 0;
                        int g = ((pv >> 8) & 0xFF) > 0 ? 1 : 0;
                        int b = (pv & 0xFF) > 0 ? 1 : 0;
                        return r > 0 || g > 0 || b > 0 ? (r << 16) | (g << 8) | b : -16777216;
                    case GRAY8:
                    case GRAY16:
                        return pv > 0 ? 1 : 0;
                }
                throw new IllegalStateException("Cannot convert image type " + pt + " to signal");
            }
        };
    }

    ColorTransformation thenApplyColorTransformation(Function<Integer, Integer> colorTransformation) {
        ColorTransformation currentTransformation = this;
        return new ColorTransformation(pixelTypeChange) {
            @Override
            public Integer apply(ImageType pt, Integer pv) {
                return colorTransformation.apply(currentTransformation.apply(pt, pv));
            }
        };
    }
}
