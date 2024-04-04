package org.janelia.colormipsearch.api_v2.bdssearch;

import net.imglib2.Cursor;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.imglib2.RandomAccess;

public class CanvasSizeChanger {
    public static < T extends RealType< T >> Img<T> changeCanvasSize(Img<T> originalImage, long newWidth, long newHeight, long xOffset, long yOffset) {
        // Create a new image with the new canvas size
        Img<T> newCanvasImage = originalImage.factory().create(newWidth, newHeight);

        long[] min = new long[]{xOffset, yOffset}; // Starting coordinates of the rectangle
        long[] max = new long[]{xOffset+newWidth, yOffset+newHeight}; // Ending coordinates of the rectangle

        // Create a view on the specified interval
        IntervalView<T> srcRegion = Views.interval(originalImage, min, max);

        Cursor<T> srcCursor = srcRegion.cursor();
        Cursor<T> destCursor = newCanvasImage.cursor();
        while (srcCursor.hasNext() && destCursor.hasNext()) {
            destCursor.next().set(srcCursor.next().copy());
        }

        return newCanvasImage;
    }
}