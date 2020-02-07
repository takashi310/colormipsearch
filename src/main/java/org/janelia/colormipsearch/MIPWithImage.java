package org.janelia.colormipsearch;

import java.util.stream.IntStream;

import ij.ImagePlus;
import ij.process.ImageProcessor;

class MIPWithImage extends MinimalColorDepthMIP {

    int width;
    int height;
    int[] pixels;

    MIPWithImage(MinimalColorDepthMIP mipInfo, ImagePlus image) {
        this.id = mipInfo.id;
        this.filepath = mipInfo.filepath;
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
