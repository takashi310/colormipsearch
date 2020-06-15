package org.janelia.colormipsearch.api.imageprocessing;

import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 * Utils for lazy image data type
 */
public class LImageUtils {

    public static LImage create(ImageArray imageArray) {
        return new LImageImpl(imageArray.type, imageArray.width, imageArray.height, imageArray::getPixel);
    }

    /**
     * Combine 2 images, pixel wise, using the given operator
     * @param l1
     * @param l2
     * @param op
     * @return
     */
    public static LImage combine2(LImage l1, LImage l2, BinaryOperator<Integer> op) {
        return new LImageImpl(l1.getPixelType(), l1.width(), l1.height(), (x, y) -> op.apply(l1.get(x, y), l2.get(x, y)));
    }

    /**
     * Combine 2 images, pixel wise, evaluating the pixels lazily
     * @param l1
     * @param l2
     * @param op
     * @return
     */
    public static LImage lazyCombine2(LImage l1, LImage l2, BiFunction<Supplier<Integer>, Supplier<Integer>, Integer> op) {
        return new LImageImpl(l1.getPixelType(), l1.width(), l1.height(), (x, y) -> op.apply(() -> l1.get(x, y), () -> l2.get(x, y)));
    }

    /**
     * Combine 3 images, pixel wise using the given operator.
     *
     * @param l1
     * @param l2
     * @param l3
     * @param op
     * @return
     */
    public static LImage lazyCombine3(LImage l1, LImage l2, LImage l3, TriFunction<Supplier<Integer>, Supplier<Integer>, Supplier<Integer>, Integer> op) {
        return new LImageImpl(l1.getPixelType(), l1.width(), l1.height(), (x, y) -> op.apply(() -> l1.get(x, y), () -> l2.get(x, y), () -> l3.get(x, y)));
    }

}
