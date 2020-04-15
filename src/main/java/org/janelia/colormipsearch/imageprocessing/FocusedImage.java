package org.janelia.colormipsearch.imageprocessing;

import java.util.Arrays;
import java.util.function.Function;

public class FocusedImage {

    public static FocusedImage fromImageArray(ImageArray imageArray) {
        return new FocusedImage(LImage.create(imageArray), 0, 0);
    }

    public static Function<FocusedImage, Integer> horizontalMirror() {
        return fImage -> fImage.lImage.get(fImage.lImage.width() - fImage.x - 1, fImage.y);
    }

    public static Function<FocusedImage, Integer> maxFilter(double radius) {
        return new Function<FocusedImage, Integer>() {
            int[] radii = makeLineRadii(radius);
            int kRadius = radii[radii.length - 1];
            int kHeight = (radii.length - 1) / 2;

            ImageTransformation.ColorHistogram histogram = null;
            int[] imageCache = null;
            @Override
            public Integer apply(FocusedImage fImage) {
                if (histogram == null) {
                    histogram = fImage.lImage.getPixelType() == ImageType.RGB ? new ImageTransformation.RGBHistogram() : new ImageTransformation.Gray8Histogram();
                    imageCache = new int[kHeight * fImage.lImage.width()];
                }
                int m = -1;
                if (fImage.x == 0) {
                    histogram.clear();
                    Arrays.fill(imageCache, 0);
                    for (int h = 0; h < kHeight; h++) {
                        int ay = fImage.y - kRadius + h;
                        if (ay >= 0 && ay < fImage.lImage.height()) {
                            for (int dx = 0; dx < radii[2 * h + 1]; dx++) {
                                int ax = fImage.x + dx;
                                if (ax < fImage.lImage.width()) {
                                    int p = fImage.lImage.get(ax, ay);
                                    imageCache[h * fImage.lImage.width() + ax] = p;
                                    m = histogram.add(p);
                                } else {
                                    break;
                                }
                            }
                        }
                    }
                }
                for (int h = 0; h < kHeight; h++) {
                    int ay = fImage.y - kRadius + h;
                    int nextx = fImage.x + radii[2 * h + 1];
                    int prevx = fImage.x + radii[2 * h] - 1;
                    if (ay >= 0 && ay < fImage.lImage.height()) {
                        if (nextx < fImage.lImage.width()) {
                            int p = fImage.lImage.get(nextx, ay);
                            imageCache[h * fImage.lImage.width() + nextx] = p;
                            m = histogram.add(p);
                        }
                        if (prevx >= 0) {
                            int p = imageCache[h * fImage.lImage.width() + prevx];
                            m = histogram.remove(p);
                        }
                    }
                }
                return m;
            }
        };
    }

    private static int[] makeLineRadii(double radiusArg) {
        double radius;
        // this code creates the same sizes as IJ.RankFilters
        if (radiusArg >= 1.5 && radiusArg < 1.75)
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
        for (int y = 1; y <= kRadius; y++) {
            // lines above and below center together
            int dx = (int) (Math.sqrt(r2 - y * y + 1e-10));
            kernel[2 * (kRadius - y)] = -dx;
            kernel[2 * (kRadius - y) + 1] = dx;
            kernel[2 * (kRadius + y)] = -dx;
            kernel[2 * (kRadius + y) + 1] = dx;
        }
        kernel[kernel.length - 1] = kRadius;
        return kernel;
    }

    private LImage lImage;
    private int x;
    private int y;

    FocusedImage(LImage lImage, int x, int y) {
        this.lImage = lImage;
        this.x = x;
        this.y = y;
    }

    int extract() {
        return lImage.get(x, y);
    }

    FocusedImage extend(Function<FocusedImage, Integer> f) {
        LImage newLImage = new LImage(
                this.lImage.getPixelType(),
                this.lImage.width(),
                this.lImage.height(),
                (i, j) -> {
                    x = i;
                    y = j;
                    return f.apply(this);
                });
        return new FocusedImage(newLImage, x, y);
    }

    int get(int relx, int rely) {
        return lImage.get(x + relx, y + rely);
    }

    ImageArray toImageArray() {
        return lImage.asImageArray();
    }
}
