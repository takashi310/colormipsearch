package org.janelia.colormipsearch.imageprocessing;

import java.util.Arrays;
import java.util.function.Function;

public class CImageProcessing {

    static CImage fromImageArray(ImageArray imageArray) {
        return new CImage(LImage.create(imageArray), 0, 0);
    }

    static Function<CImage, Integer> horizontalMirror() {
        return cImage -> cImage.lImage.get(cImage.lImage.width() - cImage.x - 1, cImage.y);
    }

    static Function<CImage, CImage> maskAndDilate(int threshold, double radius) {
        return cImage -> cImage.fmap(ColorTransformation.mask(threshold)).extend(maxFilter(radius));
    }

    static Function<CImage, Integer> maxFilter(double radius) {
        int[] radii = makeLineRadii(radius);
        int kRadius = radii[radii.length - 1];
        int kHeight = (radii.length - 1) / 2;

        return cImage -> {
            if (cImage.histogram == null) {
                cImage.histogram = cImage.lImage.getPixelType() == ImageType.RGB ? new ImageTransformation.RGBHistogram() : new ImageTransformation.Gray8Histogram();
                cImage.imageCache = new int[kHeight * cImage.lImage.width()];
            }
            int m = -1;
            if (cImage.x == 0) {
                cImage.histogram.clear();
                Arrays.fill(cImage.imageCache, 0);
                for (int h = 0; h < kHeight; h++) {
                    int ay = cImage.y - kRadius + h;
                    if (ay >= 0 && ay < cImage.lImage.height()) {
                        for (int dx = 0; dx < radii[2 * h + 1]; dx++) {
                            int ax = cImage.x + dx;
                            if (ax < cImage.lImage.width()) {
                                int p = cImage.lImage.get(ax, ay);
                                cImage.imageCache[h * cImage.lImage.width() + ax] = p;
                                m = cImage.histogram.add(p);
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
            for (int h = 0; h < kHeight; h++) {
                int ay = cImage.y - kRadius + h;
                int nextx = cImage.x + radii[2 * h + 1];
                int prevx = cImage.x + radii[2 * h];
                if (ay >= 0 && ay < cImage.lImage.height()) {
                    if (nextx < cImage.lImage.width()) {
                        int p = cImage.lImage.get(nextx, ay);
                        cImage.imageCache[h * cImage.lImage.width() + nextx] = p;
                        m = cImage.histogram.add(p);
                    }
                    if (prevx > 0) {
                        m = cImage.histogram.remove(cImage.imageCache[h * cImage.lImage.width() + prevx - 1]);
                    }
                }
            }
            return m;
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

}
