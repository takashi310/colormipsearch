package org.janelia.colormipsearch.imageprocessing;

import java.util.function.BinaryOperator;

public class ImageProcessing {

    public static ImageProcessing create() {
        return new ImageProcessing();
    }

    private final ImageTransformation imageTransformation;

    private ImageProcessing() {
        this(ImageTransformation.identity());
    }

    private ImageProcessing(ImageTransformation imageTransformation) {
        this.imageTransformation = imageTransformation;
    }

    public ImageProcessing mask(int threshold) {
        return new ImageProcessing(imageTransformation.andThen(ColorTransformation.mask(threshold)));
    }

    public ImageProcessing toGray16() {
        return new ImageProcessing(imageTransformation.andThen(ColorTransformation.toGray16()));
    }

    public ImageProcessing toBinary8(int threshold) {
        return new ImageProcessing(imageTransformation.andThen(ColorTransformation.toBinary8(threshold)));
    }

    public ImageProcessing toBinary16(int threshold) {
        return new ImageProcessing(imageTransformation.andThen(ColorTransformation.toBinary16(threshold)));
    }

    public ImageProcessing maxWithDiscPattern(double radius) {
        return new ImageProcessing(imageTransformation.andThen(ImageTransformation.maxWithDiscPattern(radius)));
    }

    public ImageProcessing horizontalMirror() {
        return new ImageProcessing(imageTransformation.andThen(ImageTransformation.horizontalMirror()));
    }

    public ImageProcessing toSignal() {
        return new ImageProcessing(imageTransformation.andThen(ColorTransformation.toSignal()));
    }

    public ImageProcessing thenApply(ImageProcessing after) {
        return new ImageProcessing(imageTransformation.andThen(after.imageTransformation));
    }

    public ImageProcessing thenApply(ImageTransformation f) {
        return new ImageProcessing(imageTransformation.andThen(f));
    }

    public LImage applyTo(ImageArray image) {
        return LImage.create(image).mapi(imageTransformation);
    }
}
