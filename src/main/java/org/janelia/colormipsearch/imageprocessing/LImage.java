package org.janelia.colormipsearch.imageprocessing;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

public class LImage {
    public static LImage create(ImageArray imageArray) {
        return new LImage(imageArray.type, imageArray.width, imageArray.height, (x, y) -> imageArray.getPixel(x, y));
    }

    public static LImage combine2(LImage l1, LImage l2, BinaryOperator<Integer> op) {
        return new LImage(l1.getPixelType(), l1.width(), l1.height(), (x, y) -> op.apply(l1.get(x, y), l2.get(x, y)));
    }

    public static LImage combine3(LImage l1, LImage l2, LImage l3, TriFunction<Integer, Integer, Integer, Integer> op) {
        return new LImage(l1.getPixelType(), l1.width(), l1.height(), (x, y) -> op.apply(l1.get(x, y), l2.get(x, y), l3.get(x, y)));
    }

    private final ImageType imageType;
    private final int width;
    private final int height;
    private final BiFunction<Integer, Integer, Integer> pixelSupplier;
    private final Map<ImageTransformation, LImage> transformationCache = new HashMap<>();
    final ImageProcessingContext imageProcessingContext;

    LImage(ImageType imageType, int width, int height,
           BiFunction<Integer, Integer, Integer> pixelSupplier) {
        this.imageType = imageType;
        this.width = width;
        this.height = height;
        this.pixelSupplier = pixelSupplier;
        this.imageProcessingContext = new ImageProcessingContext();
    }

    ImageType getPixelType() {
        return imageType;
    }

    public int get(int x, int y) {
        return pixelSupplier.apply(x, y);
    }

    public int height() {
        return height;
    }

    public int width() {
        return width;
    }

    public LImage map(ColorTransformation colorChange) {
        return new LImage(
                colorChange.pixelTypeChange.apply(getPixelType()), width, height,
                (x, y) -> colorChange.apply(getPixelType(), get(x, y))
        );
    }

    public LImage mapi(ImageTransformation imageTransformation) {
        if (transformationCache.get(imageTransformation) == null) {
            // cache the result to mimic referential transparency
            LImage result = new LImage(
                    imageTransformation.pixelTypeChange.apply(getPixelType()), width(), height(),
                    (x, y) -> imageTransformation.apply(this, x, y)
            );
            transformationCache.put(imageTransformation, result);
            return result;
        } else {
            return transformationCache.get(imageTransformation);
        }
    }

    public ImageArray asImageArray() {
        int[] pixels = new int[height() * width()];
        return new ImageArray(getPixelType(), width(), height(), foldi(pixels, (x, y, pv, pa) -> {pa[y * width + x] = pv; return pa;}));
    }

    public <R> R fold(R initialValue, BiFunction<Integer, R, R> acumulator) {
        R res = initialValue;
        int imageWidth = width();
        int imageHeight = height();
        for (int y = 0; y < imageHeight; y++) {
            for (int x = 0; x < imageWidth; x++) {
                res = acumulator.apply(get(x, y), res);
            }
        }
        return res;
    }

    <R> R foldi(R initialValue, QuadFunction<Integer, Integer, Integer, R, R> acumulator) {
        R res = initialValue;
        int imageWidth = width();
        int imageHeight = height();
        for (int y = 0; y < imageHeight; y++) {
            for (int x = 0; x < imageWidth; x++) {
                res = acumulator.apply(x, y, get(x, y), res);
            }
        }
        return res;
    }

}
