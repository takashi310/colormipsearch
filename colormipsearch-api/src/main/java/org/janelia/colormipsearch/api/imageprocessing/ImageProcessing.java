package org.janelia.colormipsearch.api.imageprocessing;

import java.io.Serializable;

/**
 * Wrapper for composing multiple image transformations.
 */
public class ImageProcessing implements Serializable {

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

    public ImageProcessing mask(int threshold) {
        return new ImageProcessing(imageTransformation.fmap(ColorTransformation.mask(threshold)));
    }

    public ImageProcessing toGray16() {
        return new ImageProcessing(imageTransformation.fmap(ColorTransformation.toGray16WithNoGammaCorrection()));
    }

    public ImageProcessing toBinary8(int threshold) {
        return new ImageProcessing(imageTransformation.fmap(ColorTransformation.toBinary8(threshold)));
    }

    public ImageProcessing toBinary16(int threshold) {
        return new ImageProcessing(imageTransformation.fmap(ColorTransformation.toBinary16(threshold)));
    }

    public ImageProcessing maxFilter(double radius) {
        return new ImageProcessing(imageTransformation.extend(ImageTransformation.maxFilter(radius)));
    }

    public ImageProcessing horizontalMirror() {
        return new ImageProcessing(imageTransformation.extend(ImageTransformation.horizontalMirror()));
    }

    public ImageProcessing toSignalRegions(int threshold) {
        return new ImageProcessing(imageTransformation.fmap(ColorTransformation.toSignalRegions(threshold)));
    }

    public ImageProcessing thenExtend(ImageTransformation f) {
        return new ImageProcessing(imageTransformation.extend(f));
    }

    public LImage applyTo(LImage image) {
        return image.mapi(imageTransformation);
    }

    public LImage applyTo(ImageArray<?> image) {
        return applyTo(LImageUtils.create(image));
    }
}
