package org.janelia.colormipsearch.imageprocessing;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class ImageArray {

    public ImageType type;
    public int height;
    public int width;
    public int[] pixels;

    ImageArray(ImageType type, int width, int height, int[] pixels) {
        this.height = height;
        this.width = width;
        this.type = type;
        this.pixels = pixels;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("type", type)
                .append("width", width)
                .append("height", height)
                .toString();
    }

    public int getPixelCount() {
        return width * height;
    }

    public int get(int pi) {
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

}
