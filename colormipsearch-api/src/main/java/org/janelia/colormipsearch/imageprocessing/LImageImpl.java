package org.janelia.colormipsearch.imageprocessing;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Lazy image type implementation.
 */
public class LImageImpl implements LImage {

    private final ImageType imageType;
    private final int width;
    private final int height;
    private final int leftBorder;
    private final int topBorder;
    private final int rightBorder;
    private final int bottomBorder;

    private final BiFunction<Integer, Integer, Integer> pixelSupplier;
    private final Map<Object, Object> processingContext;

    LImageImpl(ImageType imageType, int width, int height,
               int leftBorder, int topBorder, int rightBorder, int bottomBorder,
               BiFunction<Integer, Integer, Integer> pixelSupplier) {
        this.imageType = imageType;
        this.width = width;
        this.height = height;
        this.leftBorder = leftBorder;
        this.topBorder = topBorder;
        this.rightBorder = rightBorder;
        this.bottomBorder = bottomBorder;
        this.pixelSupplier = pixelSupplier;
        this.processingContext = new HashMap<>();
    }

    @Override
    public ImageType getPixelType() {
        return imageType;
    }

    @Override
    public int get(int x, int y) {
        return pixelSupplier.apply(x, y);
    }

    @Override
    public int height() {
        return this.height;
    }

    @Override
    public int width() {
        return this.width;
    }

    @Override
    public int leftBorder() {
        return this.leftBorder;
    }

    @Override
    public int topBorder() {
        return this.topBorder;
    }

    @Override
    public int rightBorder() {
        return this.rightBorder;
    }

    @Override
    public int bottomBorder() {
        return this.bottomBorder;
    }

    @Override
    public LImageImpl map(ColorTransformation colorChange) {
        return new LImageImpl(
                colorChange.pixelTypeChange.apply(getPixelType()),
                width, height,
                leftBorder, topBorder, rightBorder, bottomBorder,
                (x, y) -> colorChange.apply(getPixelType(), get(x, y))
        );
    }

    @Override
    public LImage mapi(ImageTransformation imageTransformation) {
        if (imageTransformation == ImageTransformation.IDENTITY) {
            return this;
        } else {
            return new LImageImpl(
                    imageTransformation.pixelTypeChange.apply(getPixelType()),
                    width, height,
                    leftBorder, topBorder, rightBorder, bottomBorder,
                    (x, y) -> imageTransformation.apply(this, x, y)
            );
        }
    }

    @Override
    public LImage reduce() {
        return LImageUtils.create(toImageArray());
    }

    @Override
    public Object getProcessingContext(String attrName) {
        return processingContext.get(attrName);
    }

    @Override
    public void setProcessingContext(String attrName, Object attrValue) {
        processingContext.put(attrName, attrValue);
    }

}
