package org.janelia.colormipsearch.imageprocessing;

import java.util.function.BiPredicate;

import ij.plugin.filter.RankFilters;
import ij.process.ImageProcessor;

public class ImageProcessing {

    public static ImageProcessing create(ImageTransformation imageTransformation) {
        return new ImageProcessing(imageTransformation);
    }

    public static ImageProcessing create() {
        return new ImageProcessing();
    }

    private final ImageTransformation imageTransformation;

    private ImageProcessing() {
        this(ImageTransformation.IDENTITY);
    }

    private ImageProcessing(ImageTransformation imageTransformation) {
        this.imageTransformation = imageTransformation;
    }

    public ImageProcessing clearRegion(BiPredicate<Integer, Integer> regionDefnPredicate) {
        return new ImageProcessing(imageTransformation.extend(ImageTransformation.clearRegion(regionDefnPredicate)));
    }

    public ImageProcessing mask(int threshold) {
        return new ImageProcessing(imageTransformation.fmap(ColorTransformation.mask(threshold)));
    }

    public ImageProcessing toGray16() {
        return new ImageProcessing(imageTransformation.fmap(ColorTransformation.toGray16()));
    }

    public ImageProcessing toBinary8(int threshold) {
        return new ImageProcessing(imageTransformation.fmap(ColorTransformation.toBinary8(threshold)));
    }

    public ImageProcessing toBinary16(int threshold) {
        return new ImageProcessing(imageTransformation.fmap(ColorTransformation.toBinary16(threshold)));
    }

    public ImageProcessing maxFilter(double radius) {
        return new ImageProcessing(imageTransformation.extend(ImageTransformation.maxFilterWithHistogram(radius)));
    }

    public ImageProcessing horizontalMirror() {
        return new ImageProcessing(imageTransformation.extend(ImageTransformation.horizontalMirror()));
    }

    public ImageProcessing toSignal() {
        return new ImageProcessing(imageTransformation.fmap(ColorTransformation.toSignal()));
    }

    public ImageProcessing thenExtend(ImageTransformation f) {
        return new ImageProcessing(imageTransformation.extend(f));
    }

    public LImage applyTo(LImage image) {
        return image.mapi(imageTransformation);
    }

    public LImage applyTo(ImageArray image) {
        return applyTo(LImage.create(image));
    }
}
