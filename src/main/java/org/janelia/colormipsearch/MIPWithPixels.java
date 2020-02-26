package org.janelia.colormipsearch;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;

import ij.ImagePlus;
import ij.plugin.filter.RankFilters;
import ij.process.ByteProcessor;
import ij.process.ColorProcessor;
import ij.process.ImageConverter;
import ij.process.ImageProcessor;
import ij.process.ShortProcessor;

class MIPWithPixels extends MIPInfo {

    int width;
    int height;
    int type;
    int[] pixels;

    private MIPWithPixels(MIPInfo mipInfo, int width, int height, int type, int[] pixels) {
        super(mipInfo);
        this.width = width;
        this.height = height;
        this.type = type;
        this.pixels = pixels;
    }

    MIPWithPixels(MIPInfo mipInfo, ImagePlus image) {
        super(mipInfo);
        ImageProcessor ip = image.getProcessor();
        this.width = ip.getWidth();
        this.height = ip.getHeight();
        this.type = image.getType();
        this.pixels = new int[width * height];
        for (int pi = 0; pi < width * height; pi++) {
            pixels[pi] = ip.get(pi);
        }
    }

    int getPixelCount() {
        return width * height;
    }

    int get(int pi) {
        return pixels[pi];
    }

    void set(int pi, int pixel) {
        pixels[pi] = pixel;
    }

    int getPixel(int x, int y) {
        if (x >= 0 && x < width && y >= 0 && y < height) {
            return pixels[y * width + x];
        } else {
            return 0;
        }
    }

    void setPixel(int x, int y, int p) {
        pixels[y * width + x] = p;
    }

    ImagePlus cloneAsImagePlus() {
        switch (this.type) {
            case ImagePlus.GRAY8:
                byte[] byteImageBuffer = new byte[this.pixels.length];
                for (int i = 0; i < pixels.length; i++) {
                    byteImageBuffer[i] = (byte) (pixels[i] & 0xFF);
                }
                return new ImagePlus(null, new ByteProcessor(width, height, byteImageBuffer));
            case ImagePlus.GRAY16:
                short[] shortImageBuffer = new short[this.pixels.length];
                for (int i = 0; i < pixels.length; i++) {
                    shortImageBuffer[i] = (byte) (pixels[i] & 0xFFFF);
                }
                return new ImagePlus(null, new ShortProcessor(width, height, shortImageBuffer, null));
            default:
                return new ImagePlus(null, new ColorProcessor(width, height, Arrays.copyOf(pixels, pixels.length)));
        }
    }

    MIPWithPixels asGray8() {
        return new MIPWithPixels(this, width, height, ImagePlus.GRAY8, streamAsGrayPixels().toArray());
//        ImagePlus image = cloneAsImagePlus();
//        ImageConverter ic = new ImageConverter(image);
//        ic.convertToGray8();
//        return new MIPWithPixels(this, image);
    }

    MIPWithPixels asBinary(int threshold) {
        int[] binaryPixels = streamAsGrayPixels()
                .map(p -> p > threshold ? 255 : 0)
                .toArray();
        return new MIPWithPixels(this, this.width, this.height, ImagePlus.GRAY8, binaryPixels);
    }

    private IntStream streamAsGrayPixels() {
        double[] w = ColorProcessor.getWeightingFactors();
        return Arrays.stream(pixels)
                .map(rgb -> {
                    int r = (rgb >> 16) & 0xFF;
                    int g = (rgb >> 8) & 0xFF;
                    int b = (rgb & 0xFF);

                    // Normalize and gamma correct:
                    float rr = (float) Math.pow(r / 255.0, 2.2);
                    float gg = (float) Math.pow(g / 255.0, 2.2);
                    float bb = (float) Math.pow(b / 255.0, 2.2);

                    // Calculate luminance:
                    double lum = w[0] * r + w[1] * g + w[2] * b + 0.5;

                    return (byte) lum;
                });
    }

    MIPWithPixels rgbMask(int threshold) {
        int[] maskPixels = Arrays.stream(pixels)
                .map(rgb -> {
                    int r = (rgb >> 16) & 0xFF;
                    int g = (rgb >> 8) & 0xFF;
                    int b = (rgb & 0xFF);

                    if (r <= threshold && g <= threshold && b <= threshold)
                        return -16777216; // alpha mask
                    else
                        return rgb;
                })
                .toArray();
        return new MIPWithPixels(this, this.width, this.height, this.type, maskPixels);
    }

    void applyMask(MIPWithPixels mask) {
        Preconditions.checkArgument(this.pixels.length == mask.pixels.length && this.width == mask.width && this.height == mask.height);
        for (int pi = 0; pi < pixels.length; pi++) {
            int p = this.pixels[pi];
            int m = mask.pixels[pi];
            // if current pixel is not black but the mask is 0 then make the pixel black
            // for ARGB black is -16777216 (alpha mask)
            if (type == ImagePlus.COLOR_RGB) {
                if (p != -16777216 && m == 0) {
                    this.pixels[pi] = -16777216;
                }
            } else {
                if (p > 0 && m == 0) {
                    this.pixels[pi] = 0;
                }
            }
        }
    }

    MIPWithPixels max2DFilter(int radius) {
        MIPWithPixels target = new MIPWithPixels(this, this.width, this.height, this.type, Arrays.copyOf(this.pixels, this.pixels.length));
        ImageProcessor imageProcessor = new ColorProcessor(target.width, target.height, target.pixels);
        RankFilters maxFilter = new RankFilters();
        maxFilter.rank(imageProcessor, radius, RankFilters.MAX);
        return target;
    }

    MIPWithPixels rawMinMax2DFilter(int radius, Comparator<Integer> pixComparator) {
        MIPWithPixels target = new MIPWithPixels(this, this.width, this.height, this.type, new int[this.pixels.length]);

        int windowWidth = 2 * radius + 1;
        Deque<Integer> wedge = new ArrayDeque<>();

        for (int y = 0; y < height; y++) {
            int xSource = 0;
            int xTarget = 0;
            int xStart = 0;

            // Step 1 - start pushing the data into the wedge
            for (; xSource < radius; ++xSource) {
                int pix = getPixel(xSource, y);
                while (!wedge.isEmpty() && pixComparator.compare(this.getPixel(wedge.getLast(), y), pix) <= 0) {
                    wedge.removeLast();
                }
                wedge.addLast(xSource);
            }

            // Step 2 - keep on pushing data, start writing to the target
            for (; xSource < width; ++xSource, ++xTarget) {
                int pix = getPixel(xSource, y);
                while (!wedge.isEmpty() && pixComparator.compare(this.getPixel(wedge.getLast(), y), pix) <= 0) {
                    wedge.removeLast();
                }
                wedge.addLast(xSource);

                target.setPixel(xTarget, y, getPixel(wedge.getFirst(), y));

                if (xSource + 1 >= windowWidth) {
                    if (wedge.getFirst() == xStart) {
                        wedge.removeFirst();
                    }
                    ++xStart;
                }
            }

            // Step 3 - Keep on removing values from the queue and keep on writing output
            // pixels cannot be pushed anymore
            for (; xTarget < width; ++xTarget) {
                target.setPixel(xTarget, y, getPixel(wedge.getFirst(), y));
                if (wedge.getFirst() == xStart) {
                    wedge.removeFirst();
                }
                ++xStart;
            }
            // clear the wedge before going to the next row
            wedge.clear();
        }
        return target;
    }

//    void maxFilter(int radius) {
//        int finalpix = 1;
//        int inivalue = 255;
//
//        for (int r = 0; r < radius; r++) {
//            int x;
//            int y;
//            for (x = 1; x < width - 1; x++) {
//                for (y = 1; y < height - 1; y++) {
//                    double pix = getPixel(x, y);
//                    /*
//                      pix5  pix2  pix8
//                      pix4  pix   pix7
//                      pix6  pix3  pix9
//                     */
//                    if (pix == 0) {
//                        double pix2 = getPixel(x, y - 1);
//                        double pix3 = getPixel(x, y + 1);
//
//                        double pix4 = getPixel(x - 1, y);
//                        double pix5 = getPixel(x - 1, y - 1);
//                        double pix6 = getPixel(x - 1, y + 1);
//
//                        double pix7 = getPixel(x + 1, y);
//                        double pix8 = getPixel(x + 1, y - 1);
//                        double pix9 = getPixel(x + 1, y + 1);
//
//                        if (pix2 == inivalue)
//                            setPixel(x, y, finalpix);
//                        else if (pix3 == inivalue)
//                            setPixel(x, y, finalpix);
//                        else if (pix4 == inivalue)
//                            setPixel(x, y, finalpix);
//                        else if (pix7 == inivalue)
//                            setPixel(x, y, finalpix);
//                        else if (pix5 == inivalue)
//                            setPixel(x, y, finalpix);
//                        else if (pix6 == inivalue)
//                            setPixel(x, y, finalpix);
//                        else if (pix8 == inivalue)
//                            setPixel(x, y, finalpix);
//                        else if (pix9 == inivalue)
//                            setPixel(x, y, finalpix);
//                    }
//
//                }
//            }
//
//            // handle first col
//            x = 0;
//            for (y = 1; y < height - 1; y++) {
//                double pix = getPixel(x, y);
//
//                if (pix == 0) {
//                    double pix2 = getPixel(x, y - 1);
//                    double pix3 = getPixel(x, y + 1);
//
//                    double pix7 = getPixel(x + 1, y);
//                    double pix8 = getPixel(x + 1, y - 1);
//                    double pix9 = getPixel(x + 1, y + 1);
//
//                    if (pix2 == inivalue)
//                        setPixel(x, y, finalpix);
//                    else if (pix3 == inivalue)
//                        setPixel(x, y, finalpix);
//                    else if (pix7 == inivalue)
//                        setPixel(x, y, finalpix);
//                    else if (pix8 == inivalue)
//                        setPixel(x, y, finalpix);
//                    else if (pix9 == inivalue)
//                        setPixel(x, y, finalpix);
//                }
//            }
//
//            // handle last col
//            x = width -1;
//            for (y = 1; y < height - 1; y++) {
//                double pix = getPixel(x, y);
//
//                if (pix == 0) {
//                    double pix2 = getPixel(x, y - 1);
//                    double pix3 = getPixel(x, y + 1);
//
//                    double pix4 = getPixel(x - 1, y);
//                    double pix5 = getPixel(x - 1, y - 1);
//                    double pix6 = getPixel(x - 1, y + 1);
//
//                    if (pix2 == inivalue)
//                        setPixel(x, y, finalpix);
//                    else if (pix3 == inivalue)
//                        setPixel(x, y, finalpix);
//                    else if (pix4 == inivalue)
//                        setPixel(x, y, finalpix);
//                    else if (pix5 == inivalue)
//                        setPixel(x, y, finalpix);
//                    else if (pix6 == inivalue)
//                        setPixel(x, y, finalpix);
//                }
//            }
//
//            // first row
//            y = 0;
//            for (x = 1; x < width - 1; x++) {
//
//                double pix = getPixel(x, y);
//
//                if (pix == inivalue) {
//                    double pix3 = getPixel(x, y + 1);
//                    double pix4 = getPixel(x - 1, y);
//                    double pix6 = getPixel(x - 1, y + 1);
//                    double pix7 = getPixel(x + 1, y);
//                    double pix9 = getPixel(x + 1, y + 1);
//
//                    if (pix3 == inivalue)
//                        setPixel(x, y, finalpix);
//                    else if (pix4 == inivalue)
//                        setPixel(x, y, finalpix);
//                    else if (pix7 == inivalue)
//                        setPixel(x, y, finalpix);
//                    else if (pix6 == inivalue)
//                        setPixel(x, y, finalpix);
//                    else if (pix9 == inivalue)
//                        setPixel(x, y, finalpix);
//                }
//            }
//
//            // last row
//            y = height - 1;
//            for (x = 1; x < width - 1; x++) {
//
//                double pix = getPixel(x, y);
//
//                if (pix == inivalue) {
//                    double pix2 = getPixel(x, y - 1);
//                    double pix4 = getPixel(x - 1, y);
//                    double pix5 = getPixel(x - 1, y - 1);
//                    double pix7 = getPixel(x + 1, y);
//                    double pix8 = getPixel(x + 1, y - 1);
//
//                    if (pix2 == inivalue)
//                        setPixel(x, y, finalpix);
//                    else if (pix4 == inivalue)
//                        setPixel(x, y, finalpix);
//                    else if (pix5 == inivalue)
//                        setPixel(x, y, finalpix);
//                    else if (pix7 == inivalue)
//                        setPixel(x, y, finalpix);
//                    else if (pix8 == inivalue)
//                        setPixel(x, y, finalpix);
//                }
//            }
//
//        }
//    }
}
