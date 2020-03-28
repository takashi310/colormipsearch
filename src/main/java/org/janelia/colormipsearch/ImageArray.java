package org.janelia.colormipsearch;

import java.util.Arrays;

import ij.ImagePlus;
import ij.process.ByteProcessor;
import ij.process.ColorProcessor;
import ij.process.ImageProcessor;
import ij.process.ShortProcessor;
import org.apache.commons.lang3.builder.ToStringBuilder;

class ImageArray {

    ImageType type;
    int height;
    int width;
    int[] pixels;

    ImageArray(ImageType type, int width, int height, int[] pixels) {
        this.height = height;
        this.width = width;
        this.type = type;
        this.pixels = pixels;
    }

    ImageArray(ImagePlus image) {
        ImageProcessor ip = image.getProcessor();
        this.type = ImageType.fromImagePlusType(image.getType());
        this.height = ip.getHeight();
        this.width = ip.getWidth();
        this.pixels = new int[width * height];
        for (int pi = 0; pi < width * height; pi++) {
            pixels[pi] = ip.get(pi);
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("type", type)
                .append("width", width)
                .append("height", height)
                .toString();
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

    ImageProcessor getImageProcessor() {
        switch (type) {
            case GRAY8:
                byte[] byteImageBuffer = new byte[pixels.length];
                for (int i = 0; i < pixels.length; i++) {
                    byteImageBuffer[i] = (byte) (pixels[i] & 0xFF);
                }
                return new ByteProcessor(width, height, byteImageBuffer);
            case GRAY16:
                short[] shortImageBuffer = new short[pixels.length];
                for (int i = 0; i < pixels.length; i++) {
                    shortImageBuffer[i] = (short) (pixels[i] & 0xFFFF);
                }
                return new ShortProcessor(width, height, shortImageBuffer, null /* default color model */);
            default:
                return new ColorProcessor(width, height, Arrays.copyOf(pixels, pixels.length));
        }
    }

}
