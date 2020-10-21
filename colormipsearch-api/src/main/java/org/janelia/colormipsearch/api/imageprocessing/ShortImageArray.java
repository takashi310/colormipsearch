package org.janelia.colormipsearch.api.imageprocessing;


public class ShortImageArray extends ImageArray {

    protected short[] pixels;

    ShortImageArray(ImageType type, int width, int height, short[] pixels) {
        this.height = height;
        this.width = width;
        this.type = type;
        this.pixels = pixels;
    }

    public int get(int pi)
    {
        return pixels[pi] & 0xFFFF;
    }

    public void set(int pi, int pixel)
    {
        pixels[pi] = (short)(pixel & 0xFFFF);
    }

    public Object getPixels() {
        return (Object)pixels;
    }

    public int getPixel(int x, int y)
    {
        if (x >= 0 && x < width && y >= 0 && y < height) {
            return (int)pixels[y * width + x] & 0xFFFF;
        } else {
            return 0;
        }
    }

    public void setPixel(int x, int y, int p)
    {
        pixels[y * width + x] = (short)p;
    }

}

