package org.janelia.colormipsearch;

import java.util.Arrays;

import ij.ImagePlus;
import ij.process.ByteProcessor;
import ij.process.ColorProcessor;
import ij.process.ImageProcessor;
import ij.process.ShortProcessor;

class MIPImage {

    enum ImageType {
        UNKNOWN(-1),
        GRAY8(ImagePlus.GRAY8),
        GRAY16(ImagePlus.GRAY16),
        RGB(ImagePlus.COLOR_RGB);

        private int ipType;

        ImageType(int ipType) {
            this.ipType = ipType;
        }

        static ImageType fromImagePlusType(int ipType) {
            for (ImageType it : ImageType.values()) {
                if (it.ipType == ipType) {
                    return it;
                }
            }
            throw new IllegalArgumentException("Invalid image type: " + ipType);
        }
    }

    MIPInfo mipInfo;
    int width;
    int height;
    ImageType type;
    int[] pixels;

    MIPImage(MIPInfo mipInfo, int width, int height, ImageType type, int[] pixels) {
        this.mipInfo = mipInfo;
        this.width = width;
        this.height = height;
        this.type = type;
        this.pixels = pixels;
    }

    MIPImage(MIPInfo mipInfo, ImagePlus image) {
        this.mipInfo = mipInfo;
        ImageProcessor ip = image.getProcessor();
        this.width = ip.getWidth();
        this.height = ip.getHeight();
        this.type = ImageType.fromImagePlusType(image.getType());
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
