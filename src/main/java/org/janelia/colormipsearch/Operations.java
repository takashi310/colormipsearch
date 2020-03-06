package org.janelia.colormipsearch;

import java.util.function.BinaryOperator;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;

class Operations {

    @FunctionalInterface
    interface ColorTransformation {
        int apply(int val);

        default ColorTransformation andThen(ColorTransformation after) {
            return val -> after.apply(apply(val));
        }
    }

    @FunctionalInterface
    interface PixelTransformation {
        int apply(int x, int y, int color);

        default PixelTransformation andThen(PixelTransformation after) {
            return (x, y, c) -> after.apply(x, y, apply(x, y, c));
        }
    }

    @FunctionalInterface
    interface ImageTransformation {
        void apply(MIPWithPixels image);

        static ImageTransformation identity() {
            return image -> {};
        }

        static ImageTransformation fromColorOp(MIPWithPixels.ImageType imageType, ColorTransformation colorTransformation) {
            return image -> {
                IntStream.range(0, image.pixels.length)
                        .forEach(pi -> {
                            image.pixels[pi] = colorTransformation.apply(image.pixels[pi]);
                        });
                image.type = imageType;
            };
        }

        static ImageTransformation fromPixelTransformation(PixelTransformation pixelTransformation) {
            return image -> {
                IntStream.range(0, image.height)
                        .forEach(y -> IntStream.range(0, image.width)
                                .forEach(x -> image.setPixel(x, y, pixelTransformation.apply(x, y, image.getPixel(x, y)))))
                ;
            };
        }

        static ImageTransformation horizontalMirrorTransformation() {
            return image -> {
                for (int x = 0; x < image.width / 2; x++) {
                    for (int y = 0; y < image.height; y++) {
                        int p1 = image.getPixel(x, y);
                        image.setPixel(x, y, image.getPixel(image.width - x - 1, y));
                        image.setPixel(image.width - x - 1, y, p1);
                    }
                }
            };
        }

        default ImageTransformation andThen(ImageTransformation after) {
            return image -> {
                apply(image);
                after.apply(image);
            };
        }

    }

    private static int rgbToGray(int rgb, float maxGrayValue) {
        int r = (rgb >> 16) & 0xFF;
        int g = (rgb >> 8) & 0xFF;
        int b = (rgb & 0xFF);

        // Normalize and gamma correct:
        float rr = (float) Math.pow(r / 255., 2.2);
        float gg = (float) Math.pow(g / 255., 2.2);
        float bb = (float) Math.pow(b / 255., 2.2);

        // Calculate luminance:
        float lum = 0.2126f * rr + 0.7152f * gg + 0.0722f * bb;

        // Gamma compand and rescale to byte range:
        return (int) (maxGrayValue * Math.pow(lum, 1.0 / 2.2));
    }

    static ColorTransformation rgbToGray8() {
        return rgb -> rgbToGray(rgb, 255);
    }

    static ColorTransformation scaleGray(float oldMax, float newMax) {
        return gray -> (int) (gray / oldMax * newMax);
    }

    static ColorTransformation rgbToGray16() {
        return rgb -> rgbToGray(rgb, 65535);
    }

    static ColorTransformation maskRGB(int threshold) {
        return val -> {
            int r = (val >> 16) & 0xFF;
            int g = (val >> 8) & 0xFF;
            int b = (val & 0xFF);

            if (r <= threshold && g <= threshold && b <= threshold)
                return -16777216; // alpha mask
            else
                return val;
        };
    }

    static ColorTransformation maskGray(int threshold) {
        return val -> val <= threshold ? 0 : val;
    }

    static ColorTransformation grayToBinary8(int threshold) {
        return val -> val <= threshold ? 0 : 255;
    }

    static ColorTransformation grayToBinary16(int threshold) {
        return val -> val <= threshold ? 0 : 65535;
    }
}
