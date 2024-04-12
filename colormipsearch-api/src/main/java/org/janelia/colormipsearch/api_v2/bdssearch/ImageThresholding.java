package org.janelia.colormipsearch.api_v2.bdssearch;

import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.list.ListImgFactory;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.logic.BitType;
import net.imglib2.Cursor;

public class ImageThresholding {
    public static <T extends IntegerType< T >> Img<T> createBinaryImage(Img<T> grayscaleImage, int lowerThreshold, int upperThreshold) {

        // Create a new binary image of the same size
        long[] dimensions = new long[grayscaleImage.numDimensions()];
        for (int d = 0; d < grayscaleImage.numDimensions(); ++d) {
            dimensions[d] = grayscaleImage.dimension(d);
        }
        Img<T> binaryImage = grayscaleImage.factory().create(dimensions);

        // Define cursors for the grayscale and binary images
        Cursor<T> grayscaleCursor = grayscaleImage.cursor();
        Cursor<T> binaryCursor = binaryImage.cursor();

        final T maxvalT = binaryImage.firstElement().createVariable();
        maxvalT.setReal( binaryImage.firstElement().getMaxValue() );
        int maxval = maxvalT.getInteger();

        // Iterate over the images and apply thresholding
        while (grayscaleCursor.hasNext() && binaryCursor.hasNext()) {
            int val = grayscaleCursor.next().getInteger();

            // Set the binary pixel based on the threshold
            binaryCursor.next().setInteger(val >= lowerThreshold && val <= upperThreshold ? maxval : 0);
        }

        return binaryImage;
    }
}