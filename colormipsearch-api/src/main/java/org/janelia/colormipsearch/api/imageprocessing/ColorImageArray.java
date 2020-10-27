package org.janelia.colormipsearch.api.imageprocessing;


public class ColorImageArray extends ImageArray<byte[]> {

    private static byte[] createPixelsArray(int w, int h, int[] pxs) {
        int npixels = w * h;
        byte[] bytePixels = new byte[npixels * 3];
        for (int i = 0; i < npixels; i++) {
            bytePixels[i * 3] = (byte) ((pxs[i] >> 16) & 0xFF);
            bytePixels[i * 3 + 1] = (byte) ((pxs[i] >> 8) & 0xFF);
            bytePixels[i * 3 + 2] = (byte) (pxs[i] & 0xFF);
        }
        return bytePixels;
    }

    ColorImageArray(ImageType type, int width, int height, byte[] pixels) {
        super(type, width, height, pixels);
    }

    ColorImageArray(ImageType type, int width, int height, int[] pixels) {
        super(type, width, height, ColorImageArray.createPixelsArray(width, height, pixels));
    }

    public int get(int pi) {
        // setting the alpha value as well
        return 0xFF000000 | (((int) pixels[pi * 3] << 16) & 0x00FF0000) | (((int) pixels[pi * 3 + 1] << 8) & 0x0000FF00) | ((int) pixels[pi * 3 + 2] & 0x000000FF);
    }

    public void set(int pi, int pixel) {
        pixels[pi * 3] = (byte) ((pixel >> 16) & 0xFF);
        pixels[pi * 3 + 1] = (byte) ((pixel >> 8) & 0xFF);
        pixels[pi * 3 + 2] = (byte) (pixel & 0xFF);
    }
}
