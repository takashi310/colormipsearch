package org.janelia.colormipsearch.api_v2.bdssearch;

import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.Cursor;

public class ImageInverter {
    private static <T extends net.imglib2.type.numeric.RealType<T>> void invertImage(Img<T> image) {
        if (image.firstElement() instanceof UnsignedByteType) {
            invertGenericImage(image, 255.0);
        } else if (image.firstElement() instanceof UnsignedShortType) {
            invertGenericImage(image, 65535.0);
        } else if (image.firstElement() instanceof FloatType) {
            invertGenericImage(image, 1.0); // Assuming the FloatType image is normalized to [0,1]
        } else {
            throw new IllegalArgumentException("Unsupported image type");
        }
    }

    private static <T extends net.imglib2.type.numeric.RealType<T>> void invertGenericImage(Img<T> image, double maxValue) {
        Cursor<T> cursor = image.cursor();
        while (cursor.hasNext()) {
            cursor.fwd();
            T type = cursor.get();
            type.setReal(maxValue - type.getRealDouble());
        }
    }

}