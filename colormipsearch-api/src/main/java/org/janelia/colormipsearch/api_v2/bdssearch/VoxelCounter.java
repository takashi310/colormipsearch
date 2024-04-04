package org.janelia.colormipsearch.api_v2.bdssearch;

import net.imglib2.Cursor;
import net.imglib2.img.Img;
import net.imglib2.type.numeric.RealType;

public class VoxelCounter {

    public static <T extends RealType<T>> long countNonZeroVoxels(Img<T> img) {
        long nonZeroCount = 0;
        Cursor<T> cursor = img.cursor();
        while (cursor.hasNext()) {
            if (cursor.next().getRealDouble() != 0) {
                nonZeroCount++;
            }
        }
        return nonZeroCount;
    }

    public static <T extends RealType<T>, S extends RealType<S>> long countZeroVoxels(Img<T> img, Img<S> mask) {
        long zeroCount = 0;
        Cursor<T> cursor = img.cursor();
        Cursor<S> mask_cursor = mask.cursor();
        while (cursor.hasNext() && mask_cursor.hasNext()) {
            if (mask_cursor.next().getRealDouble() > 200.0 && cursor.next().getRealDouble() == 0.0) {
                zeroCount++;
            }
        }
        return zeroCount;
    }

}