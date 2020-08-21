package org.janelia.colormipsearch.api.imageprocessing;


public class ColorImageArray extends ImageArray {

    protected byte[] pixels;

    ColorImageArray(ImageType type, int width, int height, byte[] pixels) {
        this.height = height;
        this.width = width;
        this.type = type;
        this.pixels = pixels;
    }

    ColorImageArray(ImageType type, int width, int height, int[] pixels) {
        this.height = height;
        this.width = width;
        this.type = type;

        this.pixels = new byte[width * height * 3];
        for (int i = 0; i < width * height; i++)
        {
            this.pixels[i*3]   = (byte)((pixels[i] >> 16) & 0xFF);
            this.pixels[i*3+1] = (byte)((pixels[i] >> 8) & 0xFF);
            this.pixels[i*3+2] = (byte)(pixels[i] & 0xFF);
        }
    }

    public int get(int pi)
    {
        return (((int)pixels[pi*3] << 16) & 0x00FF0000) | (((int)pixels[pi*3+1] << 8) & 0x0000FF00) | ((int)pixels[pi*3+2] & 0x000000FF);
    }

    public void set(int pi, int pixel)
    {
        pixels[pi*3] = (byte)((pixel >> 16) & 0xFF);
        pixels[pi*3+1] = (byte)((pixel >> 8) & 0xFF);
        pixels[pi*3+2] = (byte)(pixel & 0xFF);
    }

    public Object getPixels() {
        return (Object)pixels;
    }

    public int getPixel(int x, int y)
    {
        int id = y * width + x;
        if (x >= 0 && x < width && y >= 0 && y < height) {
            return (((int)pixels[id*3] << 16) & 0x00FF0000) | (((int)pixels[id*3+1] << 8) & 0x0000FF00) | ((int)pixels[id*3+2] & 0x000000FF);
        } else {
            return 0;
        }
    }

    public void setPixel(int x, int y, int p)
    {
        int id = y * width + x;
        pixels[id*3] = (byte)((p >> 16) & 0xFF);
        pixels[id*3+1] = (byte)((p >> 8) & 0xFF);
        pixels[id*3+2] = (byte)(p & 0xFF);
    }

}

