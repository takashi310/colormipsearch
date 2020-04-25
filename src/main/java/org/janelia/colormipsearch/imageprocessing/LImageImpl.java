package org.janelia.colormipsearch.imageprocessing;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class LImageImpl implements LImage {

    private final ImageType imageType;
    private final int width;
    private final int height;
    private final BiFunction<Integer, Integer, Integer> pixelSupplier;
    private final Map<Object, Object> processingContext;

    LImageImpl(ImageType imageType, int width, int height,
               BiFunction<Integer, Integer, Integer> pixelSupplier) {
        this.imageType = imageType;
        this.width = width;
        this.height = height;
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
        return height;
    }

    @Override
    public int width() {
        return width;
    }

    @Override
    public LImageImpl map(ColorTransformation colorChange) {
        return new LImageImpl(
                colorChange.pixelTypeChange.apply(getPixelType()), width, height,
                (x, y) -> colorChange.apply(getPixelType(), get(x, y))
        );
    }

    @Override
    public LImage mapi(ImageTransformation imageTransformation) {
        if (imageTransformation == ImageTransformation.IDENTITY) {
            return this;
        } else {
            return new LImageImpl(
                    imageTransformation.pixelTypeChange.apply(getPixelType()), width, height,
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
