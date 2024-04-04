package org.janelia.colormipsearch.imageprocessing;


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

    public ColorImageArray(ImageType type, int width, int height, byte[] pixels) {
        super(type, width, height, pixels);
    }

    public ColorImageArray(ImageType type, int width, int height, int[] pixels) {
        super(type, width, height, ColorImageArray.createPixelsArray(width, height, pixels));
    }

    public int get(int pi) {
        int r = pixels[pi * 3] & 0xFF;
        int g = pixels[pi * 3 + 1] & 0xFF;
        int b = pixels[pi * 3 + 2] & 0xFF;
        // setting the alpha value as well
        return 0xFF000000 | ((r << 16) & 0x00FF0000) | ((g << 8) & 0x0000FF00) | b;
    }

    public void set(int pi, int pixel) {
        pixels[pi * 3] = (byte) ((pixel >> 16) & 0xFF);
        pixels[pi * 3 + 1] = (byte) ((pixel >> 8) & 0xFF);
        pixels[pi * 3 + 2] = (byte) (pixel & 0xFF);
    }
}
