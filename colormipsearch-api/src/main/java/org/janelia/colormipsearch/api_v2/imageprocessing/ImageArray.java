package org.janelia.colormipsearch.api_v2.imageprocessing;

import java.io.Serializable;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Image array representation containing image type, image width, height and image pixel arrays.
 *
 * @param <T> pixel array type
 */
public abstract class ImageArray<T> implements Serializable {

    ImageType type;
    int width;
    int height;
    final T pixels;

    ImageArray(ImageType type, int width, int height, T pixels) {
        this.type = type;
        this.width = width;
        this.height = height;
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

    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }

    public abstract int get(int pi);

    public abstract void set(int pi, int pixel);

    public int getPixel(int x, int y) {
        int id = y * width + x;
        if (x >= 0 && x < width && y >= 0 && y < height) {
            return get(id);
        } else {
            return 0;
        }
    }

    public void setPixel(int x, int y, int p) {
        int id = y * width + x;
        set(id, p);
    }

    T getPixels() {
        return pixels;
    }
}
