package org.janelia.colormipsearch;

import java.util.Arrays;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;

import ij.ImagePlus;
import ij.process.ImageProcessor;

class MIPWithPixels extends MIPInfo {
    int width;
    int height;
    int type;
    int[] pixels;

    private MIPWithPixels(MIPInfo mipInfo, int type, int[] pixels) {
        super(mipInfo);
        this.type = type;
        this.pixels = pixels;
    }

    MIPWithPixels(MIPInfo mipInfo, ImagePlus image) {
        super(mipInfo);
        ImageProcessor ip = image.getProcessor();
        this.width = ip.getWidth();
        this.height = ip.getHeight();
        this.type = image.getType();
        this.pixels = new int[width * height];
        for (int pi = 0; pi < width * height; pi++) {
            pixels[pi] = ip.get(pi);
        }
    }

    int getPixelCount() {
        return width * height;
    }

    int get(int pi) {
        return pixels[pi];
    }

    void set(int pi, int pixel) {
        pixels[pi] = pixel;
    }

    int getPixel(int x, int y) {
        if (x >= 0 && x < width && y >= 0 && y < height) {
            return pixels[y * width + x];
        } else {
            return 0;
        }
    }

    void setPixel(int x, int y, int p) {
        pixels[y * width + x] = p;
    }

    MIPWithPixels asGray8() {
        int[] grayPixels = streamAsGrayPixels().toArray();
        return new MIPWithPixels(this, ImagePlus.GRAY8, grayPixels);
    }

    MIPWithPixels asBinary(int threshold) {
        int[] binaryPixels = streamAsGrayPixels()
                .map(p -> p > threshold ? 255 : 0)
                .toArray();
        return new MIPWithPixels(this, ImagePlus.GRAY8, binaryPixels);
    }

    private IntStream streamAsGrayPixels() {
        return Arrays.stream(pixels)
                .map(rgb -> {
                    int r = (rgb >> 16) & 0xFF;
                    int g = (rgb >> 8) & 0xFF;
                    int b = (rgb & 0xFF);

                    // Normalize and gamma correct:
                    float rr = (float) Math.pow(r / 255.0, 2.2);
                    float gg = (float) Math.pow(g / 255.0, 2.2);
                    float bb = (float) Math.pow(b / 255.0, 2.2);

                    // Calculate luminance:
                    float lum = 0.2126f * rr + 0.7152f * gg + 0.0722f * bb;

                    // Gamma compand and rescale to byte range:
                    return (int) (255.0 * Math.pow(lum, 1.0 / 2.2));
                });
    }

    MIPWithPixels rgbMask(int threshold) {
        int[] maskPixels = Arrays.stream(pixels)
                .map(rgb -> {
                    int r = (rgb >> 16) & 0xFF;
                    int g = (rgb >> 8) & 0xFF;
                    int b = (rgb & 0xFF);

                    if (r <= threshold && g <= threshold && b <= threshold)
                        return -16777216; // alpha mask
                    else
                        return rgb;
                })
                .toArray();
        return new MIPWithPixels(this, this.type, maskPixels);
    }

    void applyMask(MIPWithPixels mask) {
        Preconditions.checkArgument(this.pixels.length == mask.pixels.length && this.width == mask.width && this.height == mask.height);
        for (int pi = 0; pi < pixels.length; pi++) {
            int p = this.pixels[pi];
            int m = mask.pixels[pi];
            // if current pixel is not black but the mask is 0 then make the pixel black
            // for ARGB black is -16777216 (alpha mask)
            if (type == ImagePlus.COLOR_RGB) {
                if (p != -16777216 && m == 0) {
                    this.pixels[pi] = -16777216;
                }
            } else {
                if (p > 0 && m == 0) {
                    this.pixels[pi] = 0;
                }
            }
        }
    }
}
