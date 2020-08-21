package org.janelia.colormipsearch.api.imageprocessing;

public class ByteImageArray extends ImageArray {

    protected byte[] pixels;

    ByteImageArray(ImageType type, int width, int height, byte[] pixels) {
        this.height = height;
        this.width = width;
        this.type = type;
        this.pixels = pixels;
    }

    public int get(int pi)
    {
        return pixels[pi] & 0xFF;
    }

    public void set(int pi, int pixel)
    {
        pixels[pi] = (byte)pixel;
    }

    public Object getPixels() {
        return (Object)pixels;
    }

    public int getPixel(int x, int y)
    {
        if (x >= 0 && x < width && y >= 0 && y < height) {
            return (int)pixels[y * width + x];
        } else {
            return 0;
        }
    }

    public void setPixel(int x, int y, int p)
    {
        pixels[y * width + x] = (byte)p;
    }

}
