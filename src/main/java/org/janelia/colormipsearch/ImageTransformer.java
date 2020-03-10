package org.janelia.colormipsearch;

import java.util.Arrays;
import java.util.function.BinaryOperator;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;

import ij.plugin.filter.RankFilters;
import ij.process.ByteProcessor;
import ij.process.ColorProcessor;
import ij.process.ImageProcessor;
import ij.process.ShortProcessor;

class ImageTransformer {

    static ImageTransformer createFor(MIPImage image) {
        return new ImageTransformer(image);
    }

    static ImageTransformer createForDuplicate(MIPImage image) {
        return ImageTransformer.createFor(new MIPImage(image.mipInfo, image.width, image.height, image.type, Arrays.copyOf(image.pixels, image.pixels.length)));
    }

    private final MIPImage image;

    private ImageTransformer(MIPImage image) {
        this.image = image;
    }

    MIPImage getImage() {
        return image;
    }

    ImageTransformer applyImageTransformation(Operations.ImageTransformation transformation) {
        transformation.apply(image);
        return this;
    }

    ImageTransformer applyColorOp(MIPImage.ImageType imageType, Operations.ColorTransformation colorTransformation) {
        return applyImageTransformation(Operations.ImageTransformation.fromColorOp(imageType, colorTransformation));
    }


    ImageTransformer applyPixelOp(Operations.PixelTransformation pixelTransformation) {
        return applyImageTransformation(Operations.ImageTransformation.fromPixelTransformation(pixelTransformation));
    }

    ImageTransformer duplicate() {
        return ImageTransformer.createForDuplicate(image);
    }

    ImageTransformer toGray8() {
        switch (image.type) {
            case RGB:
                return applyColorOp(MIPImage.ImageType.GRAY8, Operations.rgbToGray8());
            case GRAY8:
                return this;
            case GRAY16:
                return applyColorOp(MIPImage.ImageType.GRAY8, Operations.scaleGray(65535, 255));
        }
        throw new IllegalStateException("Cannot convert " + image.type + " to gray8");
    }

    ImageTransformer toGray16() {
        switch (image.type) {
            case RGB:
                return applyColorOp(MIPImage.ImageType.GRAY16, Operations.rgbToGray16());
            case GRAY8:
                return applyColorOp(MIPImage.ImageType.GRAY16, Operations.scaleGray(255, 65535));
            case GRAY16:
                return this;
        }
        throw new IllegalStateException("Cannot convert " + image.type + " to gray16");
    }

    ImageTransformer mask(int threshold) {
        switch (image.type) {
            case RGB:
                return applyColorOp(image.type, Operations.maskRGB(threshold));
            case GRAY8:
            case GRAY16:
                return applyColorOp(image.type, Operations.maskGray(threshold));
        }
        throw new IllegalStateException("Cannot mask image type " + image.type);
    }

    ImageTransformer toBinary8(int threshold) {
        switch (image.type) {
            case RGB:
                return applyColorOp(MIPImage.ImageType.GRAY8, Operations.rgbToGray8().andThen(Operations.grayToBinary8(threshold)));
            case GRAY8:
                return applyColorOp(MIPImage.ImageType.GRAY8, Operations.grayToBinary8(threshold));
            case GRAY16:
                return applyColorOp(MIPImage.ImageType.GRAY8, Operations.scaleGray(65535, 255).andThen(Operations.grayToBinary8(threshold)));
        }
        throw new IllegalStateException("Cannot convert image type " + image.type + " to binary8");
    }

    ImageTransformer toBinary16(int threshold) {
        switch (image.type) {
            case RGB:
                return applyColorOp(MIPImage.ImageType.GRAY16, Operations.rgbToGray16().andThen(Operations.grayToBinary16(threshold)));
            case GRAY8:
                return applyColorOp(MIPImage.ImageType.GRAY16, Operations.scaleGray(255, 65535).andThen(Operations.grayToBinary16(threshold)));
            case GRAY16:
                return applyColorOp(MIPImage.ImageType.GRAY16, Operations.grayToBinary16(threshold));
        }
        throw new IllegalStateException("Cannot convert image type " + image.type + " to binary16");
    }

    ImageTransformer toNegative() {
        switch (image.type) {
            case RGB:
                return applyColorOp(image.type, val -> {
                    int r = 255 - ((val >> 16) & 0xFF);
                    int g = 255 - ((val >> 8) & 0xFF);
                    int b = 255 - (val & 0xFF);
                    return r > 0 || g > 0 || b > 0 ? (r << 16) | (g << 8) | b : -16777216;
                });
            case GRAY8:
                return applyColorOp(image.type, val -> 255 - val);
            case GRAY16:
                return applyColorOp(image.type, val -> 65535 - val);
        }
        throw new IllegalStateException("Cannot convert image type " + image.type + " to its negative");
    }

    ImageTransformer toSignal() {
        switch (image.type) {
            case RGB:
                return applyColorOp(image.type, val -> {
                    int r = ((val >> 16) & 0xFF) > 0 ? 1 : 0;
                    int g = ((val >> 8) & 0xFF) > 0 ? 1 : 0;
                    int b = (val & 0xFF) > 0 ? 1 : 0;
                    return r > 0 || g > 0 || b > 0 ? (r << 16) | (g << 8) | b : -16777216;
                });
            case GRAY8:
            case GRAY16:
                return applyColorOp(image.type, val -> val > 0 ? 1 : 0);
        }
        throw new IllegalStateException("Cannot convert image type " + image.type + " to its signal");
    }

    ImageTransformer maxFilter(int radius) {
        Operations.ImageTransformation imageTransformation;
        if (radius > 0) {
            imageTransformation = im -> {
                RankFilters maxFilter = new RankFilters();
                ImageProcessor imageProcessor;
                switch (im.type) {
                    case RGB:
                        imageProcessor = new ColorProcessor(im.width, im.height, im.pixels);
                        maxFilter.rank(imageProcessor, radius, RankFilters.MAX);
                        break;
                    case GRAY8:
                        imageProcessor = new ByteProcessor(im.width, im.height);
                        for (int pi = 0; pi < im.pixels.length; pi++) {
                            imageProcessor.set(pi, im.get(pi));
                        }
                        maxFilter.rank(imageProcessor, radius, RankFilters.MAX);
                        for (int x = 0; x < im.width; x++) {
                            for (int y = 0; y < im.height; y++) {
                                im.setPixel(x, y, imageProcessor.getPixel(x, y));
                            }
                        }
                        break;
                    case GRAY16:
                        imageProcessor = new ShortProcessor(im.width, im.height);
                        for (int pi = 0; pi < im.pixels.length; pi++) {
                            imageProcessor.set(pi, im.get(pi));
                        }
                        maxFilter.rank(imageProcessor, radius, RankFilters.MAX);
                        for (int x = 0; x < im.width; x++) {
                            for (int y = 0; y < im.height; y++) {
                                im.setPixel(x, y, imageProcessor.getPixel(x, y));
                            }
                        }
                        break;
                    default:
                        throw new IllegalStateException("Image type not supported for max filter: " + im.type);
                }
            };
        } else {
            imageTransformation = im -> {};
        }
        return applyImageTransformation(imageTransformation);
    }

    ImageTransformer gradientSlice() {
        return applyImageTransformation(im -> {
            int maxValue;
            switch (im.type) {
                case GRAY8:
                    maxValue = 255;
                    break;
                case GRAY16:
                    maxValue = 65535;
                    break;
                default:
                    throw new IllegalStateException("Gradient slice only supported for gray images");
            }
            int nextmin = 0;

            boolean done = false;
            while (!done) {
                done = true;
                for (int ix = 0; ix < im.width; ix++) {
                    for (int iy = 0; iy < im.height; iy++) {
                        int pix0 = -1;
                        int pix1 = -1;
                        int pix2 = -1;
                        int pix3 = -1;
                        int pix = im.getPixel(ix, iy);

                        if (pix == maxValue) {
                            if (ix > 0)
                                pix0 = im.getPixel(ix - 1, iy);

                            if (ix < im.width - 1)
                                pix1 = im.getPixel(ix + 1, iy);

                            if (iy > 0)
                                pix2 = im.getPixel(ix, iy - 1);

                            if (iy < im.height - 1)
                                pix3 = im.getPixel(ix, iy + 1);

                            if (pix0 == nextmin || pix1 == nextmin || pix2 == nextmin || pix3 == nextmin) {
                                im.setPixel(ix, iy, nextmin + 1);
                                done = false;
                            }
                        }
                    }
                }
                nextmin = nextmin + 1;
            }
        });
    }

    ImageTransformer combineWith(BinaryOperator<Integer> pixelOp, BinaryOperator<Integer> pixelOperandSupplier) {
        return applyImageTransformation((im) -> {
            for (int y = 0; y < im.height; y++) {
                for (int x = 0; x < im.width; x++) {
                    im.setPixel(x, y, pixelOp.apply(im.getPixel(x, y), pixelOperandSupplier.apply(x, y)));
                }
            }
        });
    }

    ImageTransformer maskWith(MIPImage otherImage) {
        return combineWith((p1, p2) -> {
            int black;
            if (image.type == MIPImage.ImageType.RGB) {
                black = -16777216;
            } else {
                black = 0;
            }
            if (p1 == black) {
                return black;
            } else if (otherImage.type == MIPImage.ImageType.RGB) {
                if (p2 == -16777216) {
                    return black;
                } else {
                    return p1;
                }
            } else {
                if (p2 == 0) {
                    return black;
                } else {
                    return p1;
                }
            }
        }, otherImage::getPixel);
    }

    ImageTransformer mulWith(MIPImage otherImage) {
        Preconditions.checkArgument(image.pixels.length == otherImage.pixels.length && image.width == otherImage.width && image.height == otherImage.height);
        return combineWith((p1, p2) -> p1 * p2, otherImage::getPixel);
    }

    IntStream stream() {
        return Arrays.stream(image.pixels);
    }
}
