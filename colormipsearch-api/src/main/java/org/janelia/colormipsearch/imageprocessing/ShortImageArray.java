package org.janelia.colormipsearch.imageprocessing;


public class ShortImageArray extends ImageArray<short[]> {

    public ShortImageArray(ImageType type, int width, int height, short[] pixels) {
        super(type, width, height, pixels);
    }

    public int get(int pi) {
        return pixels[pi] & 0xFFFF;
    }

    public void set(int pi, int pixel) {
        pixels[pi] = (short) (pixel & 0xFFFF);
    }
}
