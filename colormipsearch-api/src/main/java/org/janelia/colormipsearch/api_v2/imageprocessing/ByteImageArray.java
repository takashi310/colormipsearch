package org.janelia.colormipsearch.api_v2.imageprocessing;

public class ByteImageArray extends ImageArray<byte[]> {

    ByteImageArray(ImageType type, int width, int height, byte[] pixels) {
        super(type, width, height, pixels);
    }

    public int get(int pi) {
        return pixels[pi] & 0xFF;
    }

    public void set(int pi, int pixel) {
        pixels[pi] = (byte) (pixel & 0xFF);
    }
}
