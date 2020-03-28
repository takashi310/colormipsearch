package org.janelia.colormipsearch;

import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import com.google.common.base.Preconditions;

class ImageOperations {

    @FunctionalInterface
    interface TriFunction<S, T, U, R> {
        R apply(S s, T t, U u);

        default <V> TriFunction<S, T, U, V> andThen(Function<? super R, ? extends V> after) {
            return (S s, T t, U u) -> after.apply(apply(s, t, u));
        }
    }

    @FunctionalInterface
    interface QuadFunction<P, S, T, U, R> {
        R apply(P p, S s, T t, U u);

        default <V> QuadFunction<P, S, T, U, V> andThen(Function<? super R, ? extends V> after) {
            return (P p, S s, T t, U u) -> after.apply(apply(p, s, t, u));
        }
    }

    abstract static class ColorTransformation implements BiFunction<ImageType, Integer, Integer> {
        final Function<ImageType, ImageType> pixelTypeChange;

        ColorTransformation(Function<ImageType, ImageType> pixelTypeChange) {
            this.pixelTypeChange = pixelTypeChange;
        }

        private static int maskGray(int val, int threshold) {
            return val <= threshold ? 0 : val;
        }

        private static int grayToBinary8(int val, int threshold) {
            return val <= threshold ? 0 : 255;
        }

        private static int grayToBinary16(int val, int threshold) {
            return val <= threshold ? 0 : 65535;
        }

        private static int maskRGB(int val, int threshold) {
            int r = (val >> 16) & 0xFF;
            int g = (val >> 8) & 0xFF;
            int b = (val & 0xFF);

            if (val != -16777216 && r <= threshold && g <= threshold && b <= threshold)
                return -16777216; // alpha mask
            else
                return val;
        }

        private static int rgbToGray(int rgb, float maxGrayValue) {
            if (rgb == -16777216) {
                return 0;
            } else {
                int r = (rgb >> 16) & 0xFF;
                int g = (rgb >> 8) & 0xFF;
                int b = (rgb & 0xFF);

                // Normalize and gamma correct:
                float rr = (float) Math.pow(r / 255., 2.2);
                float gg = (float) Math.pow(g / 255., 2.2);
                float bb = (float) Math.pow(b / 255., 2.2);

                // Calculate luminance:
                float lum = 0.2126f * rr + 0.7152f * gg + 0.0722f * bb;
                // Gamma compand and rescale to byte range:
                return (int) (maxGrayValue * Math.pow(lum, 1.0 / 2.2));
            }
        }

        private static int scaleGray(int gray, float oldMax, float newMax) {
            return (int) (gray / oldMax * newMax);
        }

        static ColorTransformation toGray8() {
            return new ColorTransformation(pt -> ImageType.GRAY8) {
                @Override
                public Integer apply(ImageType pt, Integer pv) {
                    switch (pt) {
                        case RGB:
                            return rgbToGray(pv, 255);
                        case GRAY8:
                            return pv;
                        case GRAY16:
                            return scaleGray(pv, 65535, 255);
                    }
                    throw new IllegalStateException("Cannot convert " + pt + " to gray8");
                }
            };
        }

        static ColorTransformation toGray16() {
            return new ColorTransformation(pt -> ImageType.GRAY16) {
                @Override
                public Integer apply(ImageType pt, Integer pv) {
                    switch (pt) {
                        case RGB:
                            return rgbToGray(pv, 65535);
                        case GRAY8:
                            return scaleGray(pv, 255, 65535);
                        case GRAY16:
                            return pv;
                    }
                    throw new IllegalStateException("Cannot convert " + pt + " to gray16");
                }
            };
        }

        static ColorTransformation mask(int threshold) {
            return new ColorTransformation(pt -> pt) {
                @Override
                public Integer apply(ImageType pt, Integer pv) {
                    switch (pt) {
                        case RGB:
                            return maskRGB(pv, threshold);
                        case GRAY8:
                        case GRAY16:
                            return maskGray(pv, threshold);
                    }
                    throw new IllegalStateException("Cannot mask image type " + pt);
                }
            };
        }

        static ColorTransformation toBinary16(int threshold) {
            return ColorTransformation.toGray16().thenApplyColorTransformation(pv -> ColorTransformation.grayToBinary16(pv, threshold));
        }

        static ColorTransformation toBinary8(int threshold) {
            return ColorTransformation.toGray8().thenApplyColorTransformation(pv -> ColorTransformation.grayToBinary8(pv, threshold));
        }

        static ColorTransformation toSignal() {
            return new ColorTransformation(pt -> pt) {
                @Override
                public Integer apply(ImageType pt, Integer pv) {
                    switch (pt) {
                        case RGB:
                            int r = ((pv >> 16) & 0xFF) > 0 ? 1 : 0;
                            int g = ((pv >> 8) & 0xFF) > 0 ? 1 : 0;
                            int b = (pv & 0xFF) > 0 ? 1 : 0;
                            return r > 0 || g > 0 || b > 0 ? (r << 16) | (g << 8) | b : -16777216;
                        case GRAY8:
                        case GRAY16:
                            return pv > 0 ? 1 : 0;
                    }
                    throw new IllegalStateException("Cannot convert image type " + pt + " to signal");
                }
            };
        }

        ColorTransformation thenApplyColorTransformation(Function<Integer, Integer> colorTransformation) {
            ColorTransformation currentTransformation = this;
            return new ColorTransformation(pixelTypeChange) {
                @Override
                public Integer apply(ImageType pt, Integer pv) {
                    return colorTransformation.apply(currentTransformation.apply(pt, pv));
                }
            };
        }
    }

    abstract static class ImageTransformation {

        static ImageTransformation identity() {
            return new ImageTransformation() {
                @Override
                public int apply(int x, int y, LImage lImage) {
                    return lImage.get(x, y);
                }
            };
        }

        static ImageTransformation horizontalMirror() {
            return new ImageTransformation() {
                @Override
                public int apply(int x, int y, LImage lImage) {
                    return lImage.get(lImage.width() - x - 1, y);
                }
            };
        }

        static ImageTransformation maxWithDiscPattern(double radius) {
            return new ImageTransformation() {
                private final int[] radii = makeLineRadii(radius);
                private final int kRadius = radii[radii.length - 1];
                private final int kHeight = (radii.length - 1) / 2;
                ColorHistogram histogram = null;
                int[] imageCache = null;

                @Override
                public int apply(int x, int y, LImage lImage) {
                    if (histogram == null) {
                        histogram = lImage.getPixelType() == ImageType.RGB ? new RGBHistogram() : new Gray8Histogram();
                        imageCache = new int[kHeight*lImage.width()];
                    }
                    int m = -1;
                    if (x == 0) {
                        histogram.clear();
                        Arrays.fill(imageCache, 0);
                        for (int h = 0; h < kHeight; h++) {
                            int ay = y - kRadius + h;
                            if (ay >= 0 && ay < lImage.height()) {
                                for (int dx = 0; dx < radii[2 * h + 1]; dx++) {
                                    int ax = x + dx;
                                    if (ax < lImage.width()) {
                                        int p = lImage.get(ax, ay);
                                        imageCache[h * lImage.width() + ax] = p;
                                        m = histogram.add(p);
                                    } else {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    for (int h = 0; h < kHeight; h++) {
                        int ay = y - kRadius + h;
                        int nextx = x + radii[2 * h + 1];
                        int prevx = x + radii[2 * h];
                        if (ay >= 0 && ay < lImage.height()) {
                            if (nextx < lImage.width()) {
                                int p = lImage.get(nextx, ay);
                                imageCache[h * lImage.width() + nextx] = p;
                                m = histogram.add(p);
                            }
                            if (prevx > 0) {
                                m = histogram.remove(imageCache[h * lImage.width() + prevx - 1]);
                            }
                        }
                    }
                    return m;
                }
            };
        }

        private static int[] makeLineRadii(double radiusArg) {
            double radius;
            if (radiusArg >= 1.5 && radiusArg < 1.75) //this code creates the same sizes as the previous RankFilters
                radius = 1.75;
            else if (radiusArg >= 2.5 && radiusArg < 2.85)
                radius = 2.85;
            else
                radius = radiusArg;
            int r2 = (int) (radius * radius) + 1;
            int kRadius = (int) (Math.sqrt(r2 + 1e-10));
            int kHeight = 2 * kRadius + 1;
            int[] kernel = new int[2 * kHeight + 1];
            kernel[2 * kRadius] = -kRadius;
            kernel[2 * kRadius + 1] = kRadius;
            for (int y = 1; y <= kRadius; y++) {        //lines above and below center together
                int dx = (int) (Math.sqrt(r2 - y * y + 1e-10));
                kernel[2 * (kRadius - y)] = -dx;
                kernel[2 * (kRadius - y) + 1] = dx;
                kernel[2 * (kRadius + y)] = -dx;
                kernel[2 * (kRadius + y) + 1] = dx;
            }
            kernel[kernel.length - 1] = kRadius;
            return kernel;
        }

        static ImageTransformation combine2(ImageTransformation it1, ImageTransformation it2, BinaryOperator<Integer> op) {
            return new ImageTransformation(
                    it1.pixelTypeChange.andThen(it2.pixelTypeChange),
                    it1.widthChange.andThen(it2.widthChange),
                    it1.heightChange.andThen(it2.heightChange)) {
                @Override
                public int apply(int x, int y, LImage lImage) {
                    return op.apply(it1.apply(x, y, lImage), it2.apply(x, y, lImage));
                }
            };
        }

        static ImageTransformation combine3(ImageTransformation it1, ImageTransformation it2, ImageTransformation it3, TriFunction<Integer, Integer, Integer, Integer> op) {
            return new ImageTransformation(
                    it1.pixelTypeChange.andThen(it2.pixelTypeChange).andThen(it3.pixelTypeChange),
                    it1.widthChange.andThen(it2.widthChange).andThen(it3.widthChange),
                    it1.heightChange.andThen(it2.heightChange).andThen(it3.heightChange)) {
                @Override
                public int apply(int x, int y, LImage lImage) {
                    return op.apply(it1.apply(x, y, lImage), it2.apply(x, y, lImage), it3.apply(x, y, lImage));
                }
            };
        }

        final Function<ImageType, ImageType> pixelTypeChange;
        final Function<Integer, Integer> heightChange;
        final Function<Integer, Integer> widthChange;

        ImageTransformation() {
            this(Function.identity(), Function.identity(), Function.identity());
        }

        ImageTransformation(Function<ImageType, ImageType> pixelTypeChange) {
            this(pixelTypeChange, Function.identity(), Function.identity());
        }

        ImageTransformation(Function<ImageType, ImageType> pixelTypeChange,
                            Function<Integer, Integer> widthChange,
                            Function<Integer, Integer> heightChange) {
            this.pixelTypeChange = pixelTypeChange;
            this.widthChange = widthChange;
            this.heightChange = heightChange;
        }

        ImageTransformation andThen(ImageTransformation pixelTransformation) {
            ImageTransformation currentTransformation = this;
            return new ImageTransformation(
                    pixelTypeChange.andThen(pixelTransformation.pixelTypeChange),
                    widthChange.andThen(pixelTransformation.widthChange),
                    heightChange.andThen(pixelTransformation.heightChange)) {

                LImage toUpdate  = null;
                @Override
                public int apply(int x, int y, LImage lImage) {
                    if (toUpdate == null) {
                        toUpdate = lImage.mapi(currentTransformation);
                    }
                    return pixelTransformation.apply(x, y, toUpdate);
                }
            };
        }

        ImageTransformation andThen(ColorTransformation colorTransformation) {
            ImageTransformation currentTransformation = this;
            return new ImageTransformation(pixelTypeChange.andThen(colorTransformation.pixelTypeChange)) {
                @Override
                int apply(int x, int y, LImage lImage) {
                    int p = currentTransformation.apply(x, y, lImage);
                    ImageType pt = currentTransformation.pixelTypeChange.apply(lImage.getPixelType());
                    return colorTransformation.apply(pt, p);
                }
            };
        }

        abstract int apply(int x, int y, LImage lImage);
    }

    private interface ColorHistogram {
        /**
         * Add a value and return the new max
         * @param val
         * @return the new max value
         */
        int add(int val);
        /**
         * Remove the value and return the new max
         * @param val
         * @return the new max value
         */
        int remove(int val);
        void clear();
    }

    private static final class RGBHistogram implements ColorHistogram {
        private final Gray8Histogram rHistogram;
        private final Gray8Histogram gHistogram;
        private final Gray8Histogram bHistogram;

        public RGBHistogram() {
            this.rHistogram = new Gray8Histogram();
            this.gHistogram = new Gray8Histogram();
            this.bHistogram = new Gray8Histogram();
        }

        @Override
        public int add(int val) {
            int maxR = rHistogram.add(val >> 16);
            int maxG = gHistogram.add(val >> 8);
            int maxB = bHistogram.add(val);
            return getColor(maxR, maxG, maxB);
        }

        private int getColor(int r, int g, int b) {
            return 0xFF000000 |
                    r << 16 |
                    g << 8 |
                    b;
        }

        @Override
        public int remove(int val) {
            int maxR = rHistogram.remove(val >> 16);
            int maxG = gHistogram.remove(val >> 8);
            int maxB = bHistogram.remove(val);
            return getColor(maxR, maxG, maxB);
        }

        @Override
        public void clear() {
            rHistogram.clear();
            gHistogram.clear();
            bHistogram.clear();
        }
    }

    private static final class Gray8Histogram implements ColorHistogram {

        private final int[] histogram;
        private int max;
        private int count;

        Gray8Histogram() {
            histogram = new int[256];
            max = -1;
            count = 0;
        }

        @Override
        public int add(int val) {
            int ci = val & 0xFF;
            histogram[ci] = histogram[ci] + 1;
            count++;
            if (ci > max) {
                max = ci;
            }
            return max;
        }

        @Override
        public int remove(int val) {
            int ci = val & 0xFF;
            count--;
            histogram[ci] = histogram[ci] - 1;
            Preconditions.checkArgument(histogram[ci] >= 0);
            Preconditions.checkArgument(count >= 0);
            if (ci == max) {
                if (count == 0) {
                    max = -1;
                } else if (histogram[max] == 0) {
                    max = -1;
                    for (int pv = ci - 1; pv >= 0; pv--) {
                        if (histogram[pv] > 0) {
                            max = pv;
                            break;
                        }
                    }
                }
            }
            return max;
        }

        @Override
        public void clear() {
            Arrays.fill(histogram, 0);
            count = 0;
            max = -1;
        }
    }

    static class LImage {
        static LImage create(ImageArray imageArray) {
            return new LImage(imageArray.type, imageArray.width, imageArray.height, (x, y) -> imageArray.getPixel(x, y));
        }

        static LImage combine2(LImage l1, LImage l2, BinaryOperator<Integer> op) {
            return new LImage(l1.getPixelType(), l1.width(), l1.height(), (x, y) -> op.apply(l1.get(x, y), l2.get(x, y)));
        }

        static LImage combine3(LImage l1, LImage l2, LImage l3, TriFunction<Integer, Integer, Integer, Integer> op) {
            return new LImage(l1.getPixelType(), l1.width(), l1.height(), (x, y) -> op.apply(l1.get(x, y), l2.get(x, y), l3.get(x, y)));
        }

        private final ImageType imageType;
        private final int width;
        private final int height;
        private final BiFunction<Integer, Integer, Integer> pixelSupplier;

        LImage(ImageType imageType, int width, int height,
               BiFunction<Integer, Integer, Integer> pixelSupplier) {
            this.imageType = imageType;
            this.width = width;
            this.height = height;
            this.pixelSupplier = pixelSupplier;
        }

        ImageType getPixelType() {
            return imageType;
        }

        int get(int x, int y) {
            return pixelSupplier.apply(x, y);
        }

        int height() {
            return height;
        }

        int width() {
            return width;
        }

        LImage map(ColorTransformation colorChange) {
            return new LImage(
                    colorChange.pixelTypeChange.apply(getPixelType()), width, height,
                    (x, y) -> colorChange.apply(getPixelType(), get(x, y))
            );
        }

        LImage mapi(ImageTransformation pixelChange) {
            return new LImage(
                    pixelChange.pixelTypeChange.apply(getPixelType()), pixelChange.widthChange.apply(width()), pixelChange.heightChange.apply(height()),
                    (x, y) -> pixelChange.apply(x, y, this)
            );
        }

        ImageArray asImageArray() {
            int[] pixels = new int[height() * width()];
            return new ImageArray(getPixelType(), width(), height(), foldi(pixels, (x, y, pv, pa) -> {pa[y * width + x] = pv; return pa;}));
        }

//        /**
//         * Copy from source.
//         * @param before
//         * @return
//         */
//        LImage init(LImage source) {
//            return new LImage(
//                    source.getPixelType(), before.width(), before.height(),
//                    before.pixelSupplier
//            );
//        }

//        LImage composeWithBiOp(LImage lImage, BinaryOperator<Integer> op) {
//            return new LImage(
//                    getPixelType(), width(), height(),
//                    (x, y) -> get(x, y),
//                    new ImageTransformation() {
//                        @Override
//                        public Integer apply(Integer x, Integer y, LImage li) {
//                            return op.apply(li.get(x, y), lImage.get(x, y));
//                        }
//                    }
//            );
//        }

//        LImage composeWithTriOp(LImage lImage1, LImage lImage2, TriFunction<Integer, Integer, Integer, Integer> op) {
//            return new LImage(
//                    getPixelType(), width(), height(),
//                    (x, y) -> get(x, y),
//                    new ImageTransformation() {
//                        @Override
//                        public Integer apply(Integer x, Integer y, LImage li) {
//                            return op.apply(li.get(x, y), lImage1.get(x, y), lImage2.get(x, y));
//                        }
//                    }
//            );
//        }

        <R> R fold(R initialValue, BiFunction<Integer, R, R> acumulator) {
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

    static class ImageProcessing {

        static ImageProcessing create() {
            return new ImageProcessing();
        }

//        static ImageProcessing createFor(ImageArray mipImage) {
//            return new ImageProcessing(new LImage(mipImage.type, mipImage.width, mipImage.height, (x, y) -> mipImage.getPixel(x, y), ImageTransformation.identity()));
//        }

        private final ImageTransformation imageTransformation;

        private ImageProcessing() {
            this(ImageTransformation.identity());
        }

        private ImageProcessing(ImageTransformation imageTransformation) {
            this.imageTransformation = imageTransformation;
        }

        ImageProcessing mask(int threshold) {
            return new ImageProcessing(imageTransformation.andThen(ColorTransformation.mask(threshold)));
        }

        ImageProcessing toGray16() {
            return new ImageProcessing(imageTransformation.andThen(ColorTransformation.toGray16()));
        }

        ImageProcessing toBinary8(int threshold) {
            return new ImageProcessing(imageTransformation.andThen(ColorTransformation.toBinary8(threshold)));
        }

        ImageProcessing toBinary16(int threshold) {
            return new ImageProcessing(imageTransformation.andThen(ColorTransformation.toBinary16(threshold)));
        }

        ImageProcessing maxWithDiscPattern(double radius) {
            return new ImageProcessing(imageTransformation.andThen(ImageTransformation.maxWithDiscPattern(radius)));
        }

        ImageProcessing horizontalMirror() {
            return new ImageProcessing(imageTransformation.andThen(ImageTransformation.horizontalMirror()));
        }

        ImageProcessing toSignal() {
            return new ImageProcessing(imageTransformation.andThen(ColorTransformation.toSignal()));
        }

        ImageProcessing thenApply(ImageProcessing after) {
            return new ImageProcessing(imageTransformation.andThen(after.imageTransformation));
        }

        ImageProcessing thenApply(ImageTransformation f) {
            return new ImageProcessing(imageTransformation.andThen(f));
        }

        ImageProcessing combineWith(ImageProcessing processing, BinaryOperator<Integer> op) {
            return new ImageProcessing(ImageTransformation.combine2(imageTransformation, processing.imageTransformation, op));
        }

        ImageProcessing combineWith2(ImageProcessing p1, ImageProcessing p2, TriFunction<Integer, Integer, Integer, Integer> op) {
            return new ImageProcessing(ImageTransformation.combine3(imageTransformation, p1.imageTransformation, p2.imageTransformation, op));
        }

        LImage applyTo(ImageArray image) {
            return LImage.create(image).mapi(imageTransformation);
        }
    }
}
