package org.janelia.colormipsearch.imageprocessing;

import java.util.Arrays;
import java.util.function.BiPredicate;
import java.util.function.Function;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ImageTransformation {

    private static Logger LOG = LoggerFactory.getLogger(ImageTransformation.class);

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

    public static ImageTransformation identity() {
        return new ImageTransformation() {
            @Override
            public int apply(LImage lImage, int x, int y) {
                return lImage.get(x, y);
            }
        };
    }

    public static ImageTransformation horizontalMirror() {
        return new ImageTransformation() {
            @Override
            int apply(LImage lImage, int x, int y) {
                return lImage.get(lImage.width() - x - 1, y);
            }
        };
    }

    public static ImageTransformation clearRegion(BiPredicate<Integer, Integer> regionDefnPredicate) {
        return new ImageTransformation() {
            @Override
            protected int apply(LImage lImage, int x, int y) {
                if (regionDefnPredicate.test(x, y)) {
                    return -16777216;
                } else {
                    return lImage.get(x, y);
                }
            }
        };
    }

    public static ImageTransformation maxFilterWithDiscPattern(double radius) {
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

                MaxFilterContext(ColorHistogram histogram, int[] imageCache) {
                    this.histogram = histogram;
                    this.imageCache = imageCache;
                }
            }
            @Override
            int apply(LImage lImage, int x, int y) {
                MaxFilterContext maxFilterContext;
                if (lImage.imageProcessingContext.get(this) == null) {
                    maxFilterContext = new MaxFilterContext(
                            lImage.getPixelType() == ImageType.RGB ? new RGBHistogram() : new Gray8Histogram(),
                            new int[kHeight * lImage.width()]
                    );
                    lImage.imageProcessingContext.set(this, maxFilterContext);
                } else {
                    maxFilterContext = (MaxFilterContext) lImage.imageProcessingContext.get(this);
                }
                int m = -1;
                if (x == 0) {
                    maxFilterContext.histogram.clear();
                    Arrays.fill(maxFilterContext.imageCache, 0);
                    for (int h = 0; h < kHeight; h++) {
                        int ay = y - kRadius + h;
                        if (ay >= 0 && ay < lImage.height()) {
                            for (int dx = 0; dx <= radii[2 * h + 1]; dx++) {
                                int ax = x + dx;
                                if (ax < lImage.width()) {
                                    int p = lImage.get(ax, ay);
                                    maxFilterContext.imageCache[h * lImage.width() + ax] = p;
                                    m = maxFilterContext.histogram.add(p);
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
                            maxFilterContext.imageCache[h * lImage.width() + nextx] = p;
                            m = maxFilterContext.histogram.add(p);
                        }
                        if (prevx > 0) {
                            try {
                                m = maxFilterContext.histogram.remove(maxFilterContext.imageCache[h * lImage.width() + prevx - 1]);
                            } catch (IllegalArgumentException e) {
                                LOG.error("Exception at ({}, {}) -> ({}, {}) : ({}, {})", x, y, lImage.width(), lImage.height(), prevx - 1, h);
                                throw e;
                            }
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
                /**
                 * this relies on lImage.mapi being referential transparent and lImage.mapi with the same parameter
                 * always returning exactly the same result.
                 */
                return pixelTransformation.apply(lImage.mapi(currentTransformation), x, y);
            }
        };
    }

    public ImageTransformation fmap(ColorTransformation colorTransformation) {
        ImageTransformation currentTransformation = this;
        return new ImageTransformation(pixelTypeChange.andThen(colorTransformation.pixelTypeChange)) {
            @Override
            int apply(LImage lImage, int x, int y) {
                int p = currentTransformation.apply(lImage, x, y);
                ImageType pt = currentTransformation.pixelTypeChange.apply(lImage.getPixelType());
                return colorTransformation.apply(pt, p);
            }
        };
    }

    abstract int apply(LImage lImage, int x, int y);
}
