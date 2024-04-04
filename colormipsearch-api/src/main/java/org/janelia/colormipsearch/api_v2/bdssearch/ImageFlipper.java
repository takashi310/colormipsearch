package org.janelia.colormipsearch.api_v2.bdssearch;

import net.imglib2.*;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.roi.util.RandomAccessibleRegionCursor;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.view.Views;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;

public class ImageFlipper {
    public static < T extends NumericType< T >> Img<T> flipHorizontally(Img<T> inputImg) {
        // Create a new image of the same size and type as the input image
        Img<T> outputImg = inputImg.factory().create(inputImg);

        // Define the transformation for flipping
        long[] dim = new long[inputImg.numDimensions()];
        inputImg.dimensions(dim);
        long width = dim[0];

        // Flip the image horizontally
        RandomAccessibleInterval<T> flipped = Views.invertAxis(inputImg, 0);

        // Use a cursor to iterate over the flipped view and print pixel values (for demonstration)
        // Iterate over the input image
        Cursor<T> inputCursor = Views.iterable(flipped).cursor();
        Cursor<T> outputCursor = outputImg.cursor();

        while (inputCursor.hasNext()) {
            inputCursor.fwd();
            outputCursor.fwd();
            outputCursor.get().set(inputCursor.get());
        }
        return outputImg;
    }
}