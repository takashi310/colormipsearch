package org.janelia.colormipsearch.api_v2.bdssearch;

import net.imglib2.Cursor;
import net.imglib2.RealPoint;
import net.imglib2.RealRandomAccessible;
import net.imglib2.algorithm.interpolation.randomaccess.BSplineInterpolatorFactory;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.img.list.ListImgFactory;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.Views;

public class Scale3DImage {
    public static < T extends RealType< T > > Img<T> scaleImage(Img<T> img, int width, int height, int depth) {
        // Create a continuous view of the image with bi-cubic interpolation
        RealRandomAccessible<T> interpolated = Views.interpolate(Views.extendBorder(img), new BSplineInterpolatorFactory<>());

        // Calculate the dimensions of the scaled image
        long[] newDimensions = new long[3];
        double[] scaleFactor = new double[3];
        newDimensions[0] = (long) width;
        newDimensions[1] = (long) height;
        newDimensions[2] = (long) depth;
        for (int d = 0; d < 3; ++d) {
            scaleFactor[d] = (double) newDimensions[d] / img.dimension(d);
        }

        // Create an empty image for the scaled result
        Img<T> scaledImg = img.factory().create(newDimensions);

        // Prepare a RealPoint to sample the continuous view
        double[] samplePoint = new double[img.numDimensions()]; // Assuming 3D
        Cursor<T> scaledCursor = scaledImg.cursor();
        while (scaledCursor.hasNext()) {
            scaledCursor.fwd();
            // Calculate the corresponding position in the original image
            scaledCursor.localize(samplePoint);
            for (int d = 0; d < 3; ++d) {
                samplePoint[d] = samplePoint[d] / scaleFactor[d];
            }
            // Sample the continuous view at the calculated position
            scaledCursor.get().setReal(interpolated.realRandomAccess().setPositionAndGet(samplePoint[0], samplePoint[1], samplePoint[2]).getRealDouble());
        };

        return scaledImg;
    }
}