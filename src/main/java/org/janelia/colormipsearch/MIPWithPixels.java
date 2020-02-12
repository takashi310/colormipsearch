package org.janelia.colormipsearch;

import ij.ImagePlus;
import ij.process.ImageProcessor;

class MIPWithPixels extends MIPInfo {
    int width;
    int height;
    int[] pixels;

    MIPWithPixels(MIPInfo mipInfo, ImagePlus image) {
        super(mipInfo);
        ImageProcessor ip = image.getProcessor();
        this.width = ip.getWidth();
        this.height = ip.getHeight();
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
}
