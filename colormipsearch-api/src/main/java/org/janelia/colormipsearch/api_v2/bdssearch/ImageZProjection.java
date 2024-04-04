package org.janelia.colormipsearch.api_v2.bdssearch;

import net.imglib2.img.Img;
import net.imglib2.img.ImgFactory;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.list.ListImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;

public class ImageZProjection {
    public static <T extends RealType<T> > Img<T> maxIntensityProjection(Img<T> image3D, long startZ) {
        // Create a 2D image for the projection (same width and height as the 3D image)
        long width = image3D.dimension(0);
        long height = image3D.dimension(1);
        Img<T> projection = image3D.factory().create(width, height);

        // Iterate over the 2D projection image
        Cursor<T> projectionCursor = projection.cursor();
        RandomAccess<T> randomAccess = image3D.randomAccess();

        while (projectionCursor.hasNext()) {
            projectionCursor.fwd();
            long x = projectionCursor.getIntPosition(0);
            long y = projectionCursor.getIntPosition(1);

            // Find the maximum intensity along the Z-axis for this x,y position
            double maxIntensity = 0;
            for (long z = startZ; z < image3D.dimension(2); z++) {
                randomAccess.setPosition(x, 0);
                randomAccess.setPosition(y, 1);
                randomAccess.setPosition(z, 2);
                maxIntensity = Math.max(maxIntensity, randomAccess.get().getRealDouble());
            }

            // Set the pixel in the projected image
            projectionCursor.get().setReal(maxIntensity);
        }

        return projection;
    }
}
