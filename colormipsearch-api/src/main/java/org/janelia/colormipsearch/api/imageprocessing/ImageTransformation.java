package org.janelia.colormipsearch.api.imageprocessing;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;

/**
 * ImageTransformation - image transformations that take both pixel value and pixel position into consideration
 */
public abstract class ImageTransformation implements Serializable {

    /**
     * Identity transformation - it returns the same pixel as in the source.
     */
    public static ImageTransformation IDENTITY = ImageTransformation.identity();

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
        int getMax();
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
        public int getMax() {
            int maxR = rHistogram.getMax();
            int maxG = gHistogram.getMax();
            int maxB = bHistogram.getMax();
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

        Gray8Histogram() {
            histogram = new int[256];
            max = 0;
        }

        @Override
        public int add(int val) {
            int ci = val & 0xFF;
            if (ci > 0) {
                histogram[ci] = histogram[ci] + 1;
                if (ci > max) {
                    max = ci;
                }
            }
            return max;
        }

        @Override
        public int remove(int val) {
            int ci = val & 0xFF;
            if (ci > 0) {
                int ciCount = histogram[ci] - 1;
                if (ciCount < 0) {
                    histogram[ci] = 0;
                } else {
                    histogram[ci] = ciCount;
                }
                if (ci == max) {
                    if (histogram[max] == 0) {
                        max = 0;
                        for (int pv = ci - 1; pv >= 0; pv--) {
                            if (histogram[pv] > 0) {
                                max = pv;
                                break;
                            }
                        }
                    }
                }
            }
            return max;
        }

        @Override
        public int getMax() {
            return max;
        }

        @Override
        public void clear() {
            Arrays.fill(histogram, 0);
            max = 0;
        }
    }

    private static ImageTransformation identity() {
        return new ImageTransformation() {
            @Override
            public int apply(LImage lImage, int x, int y) {
                return lImage.get(x, y);
            }
        };
    }

    public static BiPredicate<Integer, Integer> getLabelRegionCond(int imageWidth) {
        return (x, y) -> x >= imageWidth - 250 && y < 90;
    }

    /**
     * Image horizontal mirroring.
     *
     * @return
     */
    public static ImageTransformation horizontalMirror() {
        return new ImageTransformation() {
            @Override
            protected int apply(LImage lImage, int x, int y) {
                return lImage.get(lImage.width() - x - 1, y);
            }
        };
    }

    /**
     * Transformation that clears the image regions identified by the given predicate.
     *
     * @param regionDefnPredicate region predicate that takes the pixel x and y and returns true if the pixel is in the region, false otherwise
     * @return the ImageTransformation that clears the region specified by the given region predicate
     */
    public static ImageTransformation clearRegion(BiPredicate<Integer, Integer> regionDefnPredicate) {
        return new ImageTransformation() {
            @Override
            protected int apply(LImage lImage, int x, int y) {
                if (regionDefnPredicate.test(x, y)) {
                    return 0xFF000000;
                } else {
                    return lImage.get(x, y);
                }
            }
        };
    }

    /**
     * Returns an image transformation that applies a maximum filter with the given radius.
     *
     * @param radius filter's radius
     * @return
     */
    public static ImageTransformation maxFilter(double radius) {
        return ImageTransformation.maxFilterWithHistogram(radius);
    }

    public static ImageTransformation unsafeMaxFilter(double radius) {
        return ImageTransformation.unsafeMaxFilterWithHistogram(radius);
    }

    private static ImageTransformation maxFilterWithHistogram(double radius) {
        int[] radii = makeLineRadii(radius);
        int kRadius = radii[radii.length - 1];
        int kHeight = (radii.length - 1) / 2;

        return new ImageTransformation() {
            /**
             * MaxFilterContext is used for immproving the performance of the max filter transformation and it
             * contains the histogram for selecting the max pixel value and a cache of the image rows. This must
             * be associated both with the image and the transformation.
             */
            class MaxFilterContext {
                final ColorHistogram histogram;
                final int[] imageCache;
                boolean forward;

                MaxFilterContext(ColorHistogram histogram, int[] imageCache) {
                    this.histogram = histogram;
                    this.imageCache = imageCache;
                    this.forward = true;
                }
            }

            @Override
            protected int apply(LImage lImage, int x, int y) {
                MaxFilterContext maxFilterContext;
                String maxFilterContextEntry = "maxFilter-" + this.hashCode();
                if (lImage.getProcessingContext(maxFilterContextEntry) == null) {
                    maxFilterContext = new MaxFilterContext(
                            lImage.getPixelType() == ImageType.RGB ? new RGBHistogram() : new Gray8Histogram(),
                            new int[kHeight * lImage.width()]
                    );
                    lImage.setProcessingContext(maxFilterContextEntry, maxFilterContext);
                } else {
                    maxFilterContext = (MaxFilterContext) lImage.getProcessingContext(maxFilterContextEntry);
                }
                int m = -1;
                if (x == 0) {
                    m = initializeHistogramForForwardTraverse(lImage, x, y, maxFilterContext.histogram, maxFilterContext.imageCache);
                    maxFilterContext.forward = true;
                }
                if (m == -1 && x == lImage.width() - 1) {
                    m = initializeHistogramForBackwardTraverse(lImage, x, y, maxFilterContext.histogram, maxFilterContext.imageCache);
                    maxFilterContext.forward = false;
                }
                if (maxFilterContext.forward) {
                    m = traverseForward(lImage, x, y, maxFilterContext.histogram, maxFilterContext.imageCache);
                } else {
                    m = traverseBackward(lImage, x, y, maxFilterContext.histogram, maxFilterContext.imageCache);
                }
                return m;
            }

            int initializeHistogramForForwardTraverse(LImage lImage, int x, int y, ColorHistogram histogram, int[] imageCache) {
                int m = -1;
                histogram.clear();
                Arrays.fill(imageCache, 0);
                for (int h = 0; h < kHeight; h++) {
                    int ay = y - kRadius + h;
                    if (ay >= 0 && ay < lImage.height()) {
                        for (int dx = 0; dx < radii[2 * h + 1]; dx++) {
                            int ax = x + dx;
                            if (ax >= 0 && ax < lImage.width()) {
                                int p = lImage.get(ax, ay);
                                imageCache[h * lImage.width() + ax] = p;
                                m = histogram.add(p);
                            } else {
                                break;
                            }
                        }
                    }
                }
                return m;
            }

            int initializeHistogramForBackwardTraverse(LImage lImage, int x, int y, ColorHistogram histogram, int[] imageCache) {
                int m = -1;
                histogram.clear();
                Arrays.fill(imageCache, 0);
                for (int h = 0; h < kHeight; h++) {
                    int ay = y - kRadius + h;
                    if (ay >= 0 && ay < lImage.height()) {
                        for (int dx = radii[2*h] + 1; dx <= 0; dx++) {
                            int ax = x + dx;
                            if (ax >= 0 && ax < lImage.width()) {
                                int p = lImage.get(ax, ay);
                                imageCache[h * lImage.width() + ax] = p;
                                m = histogram.add(p);
                            }
                        }
                    }
                }
                return m;
            }

            int traverseForward(LImage lImage, int x, int y, ColorHistogram histogram, int[] imageCache) {
                int m = -1;
                for (int h = 0; h < kHeight; h++) {
                    int ay = y - kRadius + h;
                    if (ay >= 0 && ay < lImage.height()) {
                        int nextx = x + radii[2 * h + 1];
                        if (nextx < lImage.width()) {
                            int p = lImage.get(nextx, ay);
                            imageCache[h * lImage.width() + nextx] = p;
                            m = histogram.add(p);
                        }
                        int prevx = x + radii[2 * h] - 1;
                        if (prevx >= 0) {
                            int p = imageCache[h * lImage.width() + prevx];
                            m = histogram.remove(p);
                        }
                    }
                }
                return m;
            }

            int traverseBackward(LImage lImage, int x, int y, ColorHistogram histogram, int[] imageCache) {
                int m = -1;
                for (int h = 0; h < kHeight; h++) {
                    int ay = y - kRadius + h;
                    if (ay >= 0 && ay < lImage.height()) {
                        int prevx = x + radii[2 * h];
                        if (prevx >= 0) {
                            int p = lImage.get(prevx, ay);
                            imageCache[h * lImage.width() + prevx] = p;
                            m = histogram.add(p);
                        }
                        int nextx = x + radii[2 * h + 1] + 1;
                        if (nextx < lImage.width()) {
                            int p = imageCache[h * lImage.width() + nextx];
                            try {
                                m = histogram.remove(p);
                            } catch (IllegalArgumentException e) {
                                throw e;
                            }
                        }
                    }
                }
                return m;
            }

        };
    }

    private static ImageTransformation unsafeMaxFilterWithHistogram(double radius) {
        int[] radii = makeLineRadii(radius);
        int kRadius = radii[radii.length - 1];
        int kHeight = (radii.length - 1) / 2;

        return new ImageTransformation() {
            /**
             * PixelIterator is used for immproving the performance of the max filter transformation and it
             * contains the histogram for selecting the max pixel value and a cache of the image rows. This must
             * be associated both with the image and the transformation.
             */
            class PixelIterator {
                private final LImage lImage;
                private int currX;
                private int currY;
                int cachedRowsStart;
                int cachedRowsEnd;
                final int[] pixelCache;
                final ColorHistogram histogram;

                PixelIterator(LImage lImage, int startX, int startY) {
                    this.lImage = lImage;
                    currX = startX;
                    currY = startY;
                    pixelCache = new int[lImage.width() * kHeight];
                    initPixelsCache(startY - kRadius);
                    histogram = lImage.getPixelType() == ImageType.RGB ? new RGBHistogram() : new Gray8Histogram();
                    initializeHistogram(startX, startY);
                }

                void nextXY(int nextX, int nextY) {
                    if (nextY != currY) {
                        cacheNextRow(nextY);
                        initializeHistogram(nextX, nextY);
                    } else {
                        BiFunction<Integer, Integer, Integer> toAddCoord, toRemoveCoord;
                        if (nextX > currX) {
                            // left -> right
                            toAddCoord = this::nextLeftToRight;
                            toRemoveCoord = this::prevLeftToRight;
                        } else if (nextX < currX) {
                            // right -> left
                            toAddCoord = this::nextRightToLeft;
                            toRemoveCoord = this::prevRightToLeft;
                        } else {
                            // nothing to do
                            return;
                        }
                        int miny = nextY - kRadius;
                        int maxy = nextY - kRadius + kHeight;
                        for (int ay = Math.max(0, miny); ay < Math.min(lImage.height(), maxy); ay++) {
                            int h = ay - nextY + kRadius;
                            int toAddX = toAddCoord.apply(nextX, h);
                            if (toAddX < lImage.width() && toAddX >= 0) {
                                int toAddP = pixelCache[h * lImage.width() + toAddX];
                                histogram.add(toAddP);
                            }
                            int toRemoveX = toRemoveCoord.apply(nextX, h);
                            if (toRemoveX >= 0 && toRemoveX < lImage.width()) {
                                int toRemoveP = pixelCache[h * lImage.width() + toRemoveX];
                                try {
                                    histogram.remove(toRemoveP);
                                } catch (Exception e) {
                                    String message = String.format("Error removing pixel %X at (%d,%d) while handling (%d, %d)",
                                            toRemoveP, toRemoveX, ay, nextX, nextY);
                                    throw new IllegalStateException(message, e);
                                }
                            }
                        }
                    }
                    currX = nextX;
                    currY = nextY;
                }

                private int nextLeftToRight(int x, int h) {
                    return x + radii[2 * h + 1];
                }

                private int prevLeftToRight(int x, int h) {
                    return x - 1 + radii[2 * h];
                }

                private int nextRightToLeft(int x, int h) {
                    return x + radii[2 * h];
                }

                private int prevRightToLeft(int x, int h) {
                    return x + 1 + radii[2 * h + 1];
                }

                int getCurrentPixel() {
                    return histogram.getMax();
                }

                /**
                 * Initialize histogram assumes the pixel cache is initialized.
                 * @param x
                 * @param y
                 */
                private void initializeHistogram(int x, int y) {
                    histogram.clear();
                    int cachedPixelY = y - cachedRowsStart;
                    int miny = cachedPixelY - kRadius;
                    int maxy = cachedPixelY - kRadius + kHeight;
                    for (int ay = Math.max(0, miny); ay < Math.min(cachedRowsEnd-cachedRowsStart, maxy); ay++) {
                        int h = ay - cachedPixelY + kRadius;
                        int minx = x + radii[2*h];
                        int maxx = x + radii[2*h + 1];
                        for (int ax = Math.max(0, minx); ax < maxx; ax++) {
                            int p = pixelCache[ay *  lImage.width() + ax];
                            histogram.add(p);
                        }
                    }
                }

                private void initPixelsCache(int startImageRow) {
                    cachedRowsStart = startImageRow < 0 ? 0 : startImageRow;
                    cachedRowsEnd = cachedRowsStart + kHeight < lImage.height()
                                    ? cachedRowsStart + kHeight
                                    : lImage.height();
                    for (int r = cachedRowsStart; r < cachedRowsEnd; r++) {
                        cacheRow(startImageRow, r-cachedRowsStart);
                    }
                }

                private void cacheNextRow(int y) {
                    if (y >= (cachedRowsEnd - cachedRowsStart) / 2 &&  cachedRowsEnd + 1 < lImage.height()) {
                        System.arraycopy(pixelCache, lImage.width(), pixelCache, 0, pixelCache.length - lImage.width());
                        cachedRowsStart++;
                        cachedRowsEnd++;
                        // cache the next row
                        cacheRow(cachedRowsEnd, kHeight - 1);
                    }
                }

                private void cacheRow(int imageRow, int cacheRow) {
                    for (int x = 0; x < lImage.width(); x++) {
                        pixelCache[cacheRow * lImage.width() + x] = lImage.get(x, imageRow);
                    }
                }
            }

            @Override
            protected int apply(LImage lImage, int x, int y) {
                PixelIterator maxFilterPixelIterator;
                String maxFilterContextEntry = "unsafeMaxFilter-" + this.hashCode();
                if (lImage.getProcessingContext(maxFilterContextEntry) == null) {
                    maxFilterPixelIterator = new PixelIterator(lImage, x, y);
                    lImage.setProcessingContext(maxFilterContextEntry, maxFilterPixelIterator);
                } else {
                    maxFilterPixelIterator = (PixelIterator) lImage.getProcessingContext(maxFilterContextEntry);
                    maxFilterPixelIterator.nextXY(x, y);
                }
                return maxFilterPixelIterator.getCurrentPixel();
            }

        };
    }

    /**
     * Create an with the size equal 2 * Diameter. The index in the array will give us the relative y coordinate from
     * the center calculated like `index - radius'. Then for each y there are 2 positions containing
     * x (even position) and -x (odd position).
     * For example for r=10 the values look like:
     * [-1, 1, -4, 4, -6, 6, -7, 7, -8, 8, -8, 8, -9, 9, -9, 9, -9, 9, -10, 10,
     * -10, 10,
     * -10, 10, -9, 9, -9, 9, -9, 9, -8, 8, -8, 8, -7, 7, -6, 6, -4, 4, -1, 1, 10]
     *
     * @param radiusArg
     * @return
     */
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

    final Function<ImageType, ImageType> pixelTypeChange;

    ImageTransformation() {
        this(Function.identity());
    }

    ImageTransformation(Function<ImageType, ImageType> pixelTypeChange) {
        this.pixelTypeChange = pixelTypeChange;
    }

    public ImageTransformation extend(ImageTransformation pixelTransformation) {
        ImageTransformation currentTransformation = this;
        return new ImageTransformation(
                pixelTypeChange.andThen(pixelTransformation.pixelTypeChange)) {

            @Override
            public int apply(LImage lImage, int x, int y) {
                LImage updatedImage;
                String updatedImageKey = "updatedBy" + currentTransformation.hashCode();
                if (lImage.getProcessingContext(updatedImageKey) == null) {
                    updatedImage = lImage.mapi(currentTransformation);
                    lImage.setProcessingContext(updatedImageKey, updatedImage);
                } else {
                    updatedImage = (LImage) lImage.getProcessingContext(updatedImageKey);
                }
                return pixelTransformation.apply(updatedImage, x, y);
            }
        };
    }

    /**
     * Method that creates an color based transformation for each pixel irrespective of the pixel position.
     *
     * @param colorTransformation to be applied
     * @return an image transformation that applies the given color transformation to each pixel from the image.
     */
    public ImageTransformation fmap(ColorTransformation colorTransformation) {
        ImageTransformation currentTransformation = this;
        return new ImageTransformation(pixelTypeChange.andThen(colorTransformation.pixelTypeChange)) {
            @Override
            protected int apply(LImage lImage, int x, int y) {
                int p = currentTransformation.apply(lImage, x, y);
                ImageType pt = currentTransformation.pixelTypeChange.apply(lImage.getPixelType());
                return colorTransformation.apply(pt, p);
            }
        };
    }

    /**
     * @param lImage source image
     * @param x pixel x position
     * @param y pixel y position
     * @return pixel from lImage after applying the transformation at (x, y)
     */
    protected abstract int apply(LImage lImage, int x, int y);
}
