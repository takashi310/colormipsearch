package org.janelia.colormipsearch.api.imageprocessing;

import java.util.function.BiFunction;

/**
 * Lazy Image data type
 */
public interface LImage {

    /**
     * @return pixel type
     */
    ImageType getPixelType();

    /**
     * Get the pixel value at the given positions
     * @param x position
     * @param y position
     * @return pixel value
     */
    int get(int x, int y);

    /**
     * @return image height.
     */
    int height();

    /**
     * @return image width.
     */
    int width();

    /**
     * Change every pixel from this image using the given color transformation.
     *
     * @param colorChange pixel transformation
     * @return
     */
    LImage map(ColorTransformation colorChange);

    /**
     * Change every pixel from this image but the position of the pixel is also important.
     *
     * @param imageTransformation
     * @return
     */
    LImage mapi(ImageTransformation imageTransformation);

    LImage reduce();

    /**
     * Retrieve the attribute from the processing context.
     *
     * @param attrName
     * @return
     */
    Object getProcessingContext(String attrName);

    /**
     * This just a mechanism to cache something in the processing context of this image.
     *
     * @param attrName
     * @param attrValue
     */
    void setProcessingContext(String attrName, Object attrValue);

    /**
     * Fold over the pixels of the image using the initial value and the acumulator. This is equivalent to:
     *
     * <pre>{@code
     * a = initialValue
     * for p in pixels:
     *    a = accumulator.apply(p, a)
     * return a
     * }</pre>
     * @param initialValue
     * @param acumulator
     * @param <R>
     * @return
     */
    default <R> R fold(R initialValue, BiFunction<Integer, R, R> acumulator) {
        R res = initialValue;
        for (int y = 0; y < height(); y++) {
            for (int x = 0; x < width(); x++) {
                res = acumulator.apply(get(x, y), res);
            }
        }
        return res;
    }

    /**
     * Fold over the pixels using a 4 param function that takes x, y, pixel value and the cumulated value.
     *
     * @param initialValue initial value
     * @param acumulator acumulator function that takes x, y, pixel at x,y and the cumulated value
     * @param <R> result type
     * @return acumulated result
     */
    default <R> R foldi(R initialValue, QuadFunction<Integer, Integer, Integer, R, R> acumulator) {
        R res = initialValue;
        for (int y = 0; y < height(); y++) {
            for (int x = 0; x < width(); x++) {
                res = acumulator.apply(x, y, get(x, y), res);
            }
        }
        return res;
    }

    /**
     * Evaluate a lazy image as an image array.
     * @return
     */
    default ImageArray<?> toImageArray() {
        ImageType type = getPixelType();
        ImageArray<?> ret = null;
        switch (type) {
            case GRAY8:
            {
                byte[] pixels = new byte[height() * width()];
                ret = new ByteImageArray(getPixelType(), width(), height(), foldi(pixels, (x, y, pv, pa) -> {
                    pa[y * width() + x] = pv.byteValue();
                    return pa;
                }));
            }
            break;
            case GRAY16:
            {
                short[] pixels = new short[height() * width()];
                ret = new ShortImageArray(getPixelType(), width(), height(), foldi(pixels, (x, y, pv, pa) -> {
                    pa[y * width() + x] = pv.shortValue();
                    return pa;
                }));
            }
            break;
            case RGB:
            {
                int[] pixels = new int[height() * width()];
                ret = new ColorImageArray(getPixelType(), width(), height(), foldi(pixels, (x, y, pv, pa) -> {
                    pa[y * width() + x] = pv;
                    return pa;
                }));
            }
            break;
        }
        return ret;
    }

}
