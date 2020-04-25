package org.janelia.colormipsearch.imageprocessing;

import java.util.function.BiFunction;

public interface LImage {

    ImageType getPixelType();

    int get(int x, int y);

    int height();

    int width();

    LImage map(ColorTransformation colorChange);

    LImage mapi(ImageTransformation imageTransformation);

    LImage reduce();

    Object getProcessingContext(String attrName);

    void setProcessingContext(String attrName, Object attrValue);

    default <R> R fold(R initialValue, BiFunction<Integer, R, R> acumulator) {
        R res = initialValue;
        for (int y = 0; y < height(); y++) {
            for (int x = 0; x < width(); x++) {
                res = acumulator.apply(get(x, y), res);
            }
        }
        return res;
    }

    default <R> R foldi(R initialValue, QuadFunction<Integer, Integer, Integer, R, R> acumulator) {
        R res = initialValue;
        for (int y = 0; y < height(); y++) {
            for (int x = 0; x < width(); x++) {
                res = acumulator.apply(x, y, get(x, y), res);
            }
        }
        return res;
    }

    default ImageArray toImageArray() {
        int[] pixels = new int[height() * width()];
        return new ImageArray(getPixelType(), width(), height(), foldi(pixels, (x, y, pv, pa) -> {
            pa[y * width() + x] = pv;
            return pa;
        }));
    }

}
