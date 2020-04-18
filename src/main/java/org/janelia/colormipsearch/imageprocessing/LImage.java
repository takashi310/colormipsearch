package org.janelia.colormipsearch.imageprocessing;

import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.stream.IntStream;

import ij.plugin.filter.RankFilters;
import ij.process.ImageProcessor;

public class LImage {
    public static LImage create(ImageArray imageArray) {
        return new LImage(imageArray.type, imageArray.width, imageArray.height, (x, y) -> imageArray.getPixel(x, y));
    }

    public static LImage create(ImageType imageType, ImageProcessor ijProcessor) {
        return new LImage(imageType, ijProcessor.getWidth(), ijProcessor.getHeight(), (x, y) -> ijProcessor.getPixel(x, y));
    }

    public static LImage createDilatedImage(ImageArray imageArray, double radius) {
        RankFilters maxFilter = new RankFilters();
        ImageProcessor ijImageProcessor = imageArray.getImageProcessor();
        maxFilter.rank(ijImageProcessor, radius, RankFilters.MAX);
        return LImage.create(imageArray.type, ijImageProcessor);
    }

    public static LImage combine2(LImage l1, LImage l2, BinaryOperator<Integer> op) {
        return new LImage(l1.getPixelType(), l1.width, l1.height, (x, y) -> op.apply(l1.get(x, y), l2.get(x, y)));
    }

    public static LImage combine3(LImage l1, LImage l2, LImage l3, TriFunction<Integer, Integer, Integer, Integer> op) {
        return new LImage(l1.getPixelType(), l1.width, l1.height, (x, y) -> op.apply(l1.get(x, y), l2.get(x, y), l3.get(x, y)));
    }

    private final ImageType imageType;
    private final int width;
    private final int height;
    private final BiFunction<Integer, Integer, Integer> pixelSupplier;
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
        if (imageTransformation == ImageTransformation.IDENTITY) {
            return this;
        } else {
            return new LImage(
                    imageTransformation.pixelTypeChange.apply(getPixelType()), width, height,
                    (x, y) -> imageTransformation.apply(this, x, y)
            );
        }
    }

    public ImageArray asImageArray() {
        int[] pixels = new int[height * width];
        return new ImageArray(getPixelType(), width, height, foldi(pixels, (x, y, pv, pa) -> {
            pa[y * width + x] = pv;
            return pa;
        }));
    }

    public LImage apply() {
        return LImage.create(asImageArray());
    }

    public <R> R fold(R initialValue, BiFunction<Integer, R, R> acumulator) {
        R res = initialValue;
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                res = acumulator.apply(get(x, y), res);
            }
        }
        return res;
    }

    <R> R foldp(R initialValue, BiFunction<Integer, R, R> acumulator, BinaryOperator<R> combiner) {
        R res = initialValue;
        return IntStream.range(0, height).parallel()
                .flatMap(y -> IntStream.range(0, width).parallel().map(x -> get(x, y)))
                .boxed()
                .reduce(res, (r, p) -> acumulator.apply(p, r), combiner);
    }

    public <R> R foldi(R initialValue, QuadFunction<Integer, Integer, Integer, R, R> acumulator) {
        R res = initialValue;
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                res = acumulator.apply(x, y, get(x, y), res);
            }
        }
        return res;
    }

    public <R> R foldip(R initialValue, QuadFunction<Integer, Integer, Integer, R, R> acumulator, BinaryOperator<R> combiner) {
        R res = initialValue;
        return IntStream.range(0, height).parallel()
                .boxed()
                .reduce(res,
                        (r, y) -> IntStream.range(0, width).parallel()
                                .boxed()
                                .reduce(r, (r1, x) -> acumulator.apply(x, y, get(x, y), r1), combiner),
                        combiner
                );
    }

}
