package org.janelia.colormipsearch.api.imageprocessing;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.nio.ByteBuffer;

/**
 * Image array representation containing image type,  image width, height and image pixel arrays.
 */
public abstract class ImageArray {

    ImageType type;
    int height;
    int width;

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

    public abstract int getPixel(int x, int y);

    public abstract Object getPixels();

    public abstract void setPixel(int x, int y, int p);

}
