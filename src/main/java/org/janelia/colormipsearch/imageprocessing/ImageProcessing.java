package org.janelia.colormipsearch.imageprocessing;

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

    public ImageProcessing maxFilterWithDiscPattern(double radius) {
        return new ImageProcessing(imageTransformation.extend(ImageTransformation.maxFilterWithDiscPattern(radius)));
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

    public LImage applyTo(ImageArray image) {
        return LImage.create(image).mapi(imageTransformation);
    }
}
