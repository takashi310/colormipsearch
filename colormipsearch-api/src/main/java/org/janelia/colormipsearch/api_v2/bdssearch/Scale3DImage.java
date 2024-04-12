package org.janelia.colormipsearch.api_v2.bdssearch;

import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.RealRandomAccessible;
import net.imglib2.algorithm.interpolation.randomaccess.BSplineCoefficientsInterpolatorFactory;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.DoubleType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;

public class Scale3DImage {
    public static < T extends RealType< T > > Img<T> scaleImage(Img<T> img, int width, int height, int depth) {
        // Create a continuous view of the image with bi-cubic interpolation
        BSplineCoefficientsInterpolatorFactory factory = new BSplineCoefficientsInterpolatorFactory<>( img, 3, false );
        RealRandomAccessible<DoubleType> realImg = Views.interpolate( img, factory);

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
            double val = realImg.getAt(samplePoint[0], samplePoint[1], samplePoint[2]).getRealDouble();
            if (val < 0.0) val = 0.0;
            scaledCursor.get().setReal(val);
        };

        return scaledImg;
    }

    public static < T extends RealType< T >> double getTrilinearInterpolatedPixel(double x, double y, double z, int w, int h, int d, RandomAccess<T> img) {
        if (x>=-1 && x<w && y>=-1 && y<h && z>=-1 && z<d ) {
            if (x<0.0) x = 0.0;
            if (x>=w-1.0) x = w-1.001;
            if (y<0.0) y = 0.0;
            if (y>=h-1.0) y = h-1.001;
            if (z<0.0) z = 0.0;
            if (z>=d-1.0) z = d-1.001;

            int ix = (int)x;
            int iy = (int)y;
            int iz = (int)z;

            double dx = x - ix;
            double dy = y - iy;
            double dz = z - iz;

            double c000 = img.setPositionAndGet(ix, iy, iz).getRealDouble();
            double c100 = img.setPositionAndGet(ix+1, iy, iz).getRealDouble();
            double c010 = img.setPositionAndGet(ix, iy+1, iz).getRealDouble();
            double c110 = img.setPositionAndGet(ix+1, iy+1, iz).getRealDouble();
            double c001 = img.setPositionAndGet(ix, iy, iz+1).getRealDouble();
            double c101 = img.setPositionAndGet(ix+1, iy, iz+1).getRealDouble();
            double c011 = img.setPositionAndGet(ix, iy+1, iz+1).getRealDouble();
            double c111 = img.setPositionAndGet(ix+1, iy+1, iz+1).getRealDouble();

            double c00 = c000 * (1 - dx) + c100 * dx;
            double c01 = c001 * (1 - dx) + c101 * dx;
            double c10 = c010 * (1 - dx) + c110 * dx;
            double c11 = c011 * (1 - dx) + c111 * dx;

            double c0 = c00 * (1 - dy) + c10 * dy;
            double c1 = c01 * (1 - dy) + c11 * dy;

            double c = c0 * (1 - dz) + c1 * dz;

            return c;
        } else
            return 0.0;
    }

    /** This method is from Chapter 16 of "Digital Image Processing:
     An Algorithmic Introduction Using Java" by Burger and Burge
     (http://www.imagingbook.com/). */
    public static < T extends RealType< T >> double getTricubicInterpolatedPixel(double x0, double y0, double z0, int w, int h, int d, RandomAccess<T> img) {
        int u0 = (int) Math.floor(x0);	//use floor to handle negative coordinates too
        int v0 = (int) Math.floor(y0);
        int t0 = (int) Math.floor(z0);
        if (u0<=0 || u0>=w-2 || v0<=0 || v0>=h-2 || t0<=0 || t0>=d-2)
            return getTrilinearInterpolatedPixel(x0, y0, z0, w, h, d, img);

        double r = 0;
        for (int k = 0; k <= 3; k++) {
            int t = t0 - 1 + k;
            double q = 0;
            for (int j = 0; j <= 3; j++) {
                int v = v0 - 1 + j;
                double p = 0;
                for (int i = 0; i <= 3; i++) {
                    int u = u0 - 1 + i;
                    p = p + img.setPositionAndGet(u, v, t).getRealDouble() * cubic(x0 - u);
                }
                q = q + p * cubic(y0 - v);
            }
            r = r + q * cubic(z0 - t);
        }
        return r;
    }

    public static < T extends IntegerType< T >> Img<T> scaleImage2(Img<T> img, int dstWidth, int dstHeight, int dstDepth) {
        int width = (int)img.dimension(0);
        int height = (int)img.dimension(1);
        int depth = (int)img.dimension(2);

        double srcCenterX = (double)width/2.0;
        double srcCenterY = (double)height/2.0;
        double srcCenterZ = (double)depth/2.0;
        double dstCenterX = (double)dstWidth/2.0;
        double dstCenterY = (double)dstHeight/2.0;
        double dstCenterZ = (double)dstDepth/2.0;
        double xScale = (double)dstWidth/width;
        double yScale = (double)dstHeight/height;
        double zScale = (double)dstDepth/depth;

        if (dstWidth!=width) dstCenterX+=xScale/4.0;
        if (dstHeight!=height) dstCenterY+=yScale/4.0;
        if (dstDepth!=depth) dstCenterZ+=zScale/4.0;

        RandomAccess<T> imgRA = img.randomAccess();

        long[] newDimensions = new long[] {dstWidth, dstHeight, dstDepth};
        Img<T> scaledImg = img.factory().create(newDimensions);
        Cursor<T> scaledCursor = scaledImg.cursor();

        int maxval =  (int) img.firstElement().getMaxValue();

        double xs, ys, zs;
        for (int z=0; z<=dstDepth-1; z++) {
            zs = (z - dstCenterZ) / zScale + srcCenterZ;
            for (int y = 0; y <= dstHeight - 1; y++) {
                ys = (y-dstCenterY)/yScale + srcCenterY;
                for (int x=0; x<=dstWidth-1; x++) {
                    xs = (x-dstCenterX)/xScale + srcCenterX;
                    int value = (int)(getTricubicInterpolatedPixel(xs, ys, zs, width, height, depth, imgRA)+0.5);
                    if (value<0) value = 0;
                    if (value>maxval) value = maxval;
                    scaledCursor.next().setInteger(value);
                }
            }
        }

        return scaledImg;
    }

    public static < T extends RealType< T >> double getLinearInterpolatedPixelX(double x, int iy, int iz, int w, int h, int d, RandomAccess<T> img) {
        if (x>=-1 && x<w && iy>=-1 && iy<h && iz>=-1 && iz<d ) {
            if (x<0.0) x = 0.0;
            if (x>=w-1.0) x = w-1.001;

            int ix = (int)x;
            double dx = x - ix;
            double c0 = img.setPositionAndGet(ix, iy, iz).getRealDouble();
            double c1 = img.setPositionAndGet(ix+1, iy, iz).getRealDouble();
            double c = c0 * (1 - dx) + c1 * dx;

            return c;
        } else
            return 0.0;
    }

    public static < T extends RealType< T >> double getLinearInterpolatedPixelY(int ix, double y, int iz, int w, int h, int d, RandomAccess<T> img) {
        if (ix>=-1 && ix<w && y>=-1 && y<h && iz>=-1 && iz<d ) {
            if (y<0.0) y = 0.0;
            if (y>=h-1.0) y = h-1.001;

            int iy = (int)y;
            double dy = y - iy;
            double c0 = img.setPositionAndGet(ix, iy, iz).getRealDouble();
            double c1 = img.setPositionAndGet(ix, iy+1, iz).getRealDouble();
            double c = c0 * (1 - dy) + c1 * dy;

            return c;
        } else
            return 0.0;
    }

    public static < T extends RealType< T >> double getLinearInterpolatedPixelZ(int ix, int iy, double z, int w, int h, int d, RandomAccess<T> img) {
        if (ix>=-1 && ix<w && iy>=-1 && iy<h && z>=-1 && z<d ) {
            if (z<0.0) z = 0.0;
            if (z>=d-1.0) z = d-1.001;

            int iz = (int)z;
            double dz = z - iz;

            double c0 = img.setPositionAndGet(ix, iy, iz).getRealDouble();
            double c1 = img.setPositionAndGet(ix, iy, iz+1).getRealDouble();
            double c = c0 * (1 - dz) + c1 * dz;

            return c;
        } else
            return 0.0;
    }

    public static < T extends RealType< T >> double getCubicInterpolatedPixelX(double x0, int y0, int z0, int w, int h, int d, RandomAccess<T> img) {
        int u0 = (int) Math.floor(x0);	//use floor to handle negative coordinates too
        if (u0<=0 || u0>=w-2)
            return getLinearInterpolatedPixelX(x0, y0, z0, w, h, d, img);

        double p = 0;
        for (int i = 0; i <= 3; i++) {
            int u = u0 - 1 + i;
            p = p + img.setPositionAndGet(u, y0, z0).getRealDouble() * cubic(x0 - u);
        }

        return p;
    }

    public static < T extends RealType< T >> double getCubicInterpolatedPixelY(int x0, double y0, int z0, int w, int h, int d, RandomAccess<T> img) {
        int v0 = (int) Math.floor(y0);	//use floor to handle negative coordinates too
        if (v0<=0 || v0>=h-2)
            return getLinearInterpolatedPixelY(x0, y0, z0, w, h, d, img);

        double p = 0;
        for (int i = 0; i <= 3; i++) {
            int v = v0 - 1 + i;
            p = p + img.setPositionAndGet(x0, v, z0).getRealDouble() * cubic(y0 - v);
        }

        return p;
    }

    public static < T extends RealType< T >> double getCubicInterpolatedPixelZ(int x0, int y0, double z0, int w, int h, int d, RandomAccess<T> img) {
        int t0 = (int) Math.floor(z0);	//use floor to handle negative coordinates too
        if (t0<=0 || t0>=d-2)
            return getLinearInterpolatedPixelZ(x0, y0, z0, w, h, d, img);

        double p = 0;
        for (int i = 0; i <= 3; i++) {
            int t = t0 - 1 + i;
            p = p + img.setPositionAndGet(x0, y0, t).getRealDouble() * cubic(z0 - t);
        }

        return p;
    }

    static final double a = 0.5; // Catmull-Rom interpolation
    public static final double cubic(double x) {
        if (x < 0.0) x = -x;
        double z = 0.0;
        if (x < 1.0)
            z = x*x*(x*(-a+2.0) + (a-3.0)) + 1.0;
        else if (x < 2.0)
            z = -a*x*x*x + 5.0*a*x*x - 8.0*a*x + 4.0*a;
        return z;
    }

    public static < T extends IntegerType< T >> Img<T> scaleImage3(Img<T> img, int dstWidth, int dstHeight, int dstDepth) {
        int width = (int)img.dimension(0);
        int height = (int)img.dimension(1);
        int depth = (int)img.dimension(2);

        double srcCenterX = (double)width/2.0;
        double srcCenterY = (double)height/2.0;
        double srcCenterZ = (double)depth/2.0;
        double dstCenterX = (double)dstWidth/2.0;
        double dstCenterY = (double)dstHeight/2.0;
        double dstCenterZ = (double)dstDepth/2.0;
        double xScale = (double)dstWidth/width;
        double yScale = (double)dstHeight/height;
        double zScale = (double)dstDepth/depth;

        if (dstWidth!=width) dstCenterX+=xScale/4.0;
        if (dstHeight!=height) dstCenterY+=yScale/4.0;
        if (dstDepth!=depth) dstCenterZ+=zScale/4.0;

        RandomAccess<T> imgRA = img.randomAccess();

        long[] newDimensions = new long[] {dstWidth, dstHeight, dstDepth};
        long[] maxDimensions = new long[] {dstWidth > width ? dstWidth : width, dstHeight > height ? dstHeight : height, dstDepth};
        long[] tmpDimensions = new long[] {width, dstHeight, dstDepth};
        ArrayImgFactory<FloatType> factory = new ArrayImgFactory<>(new FloatType());
        Img<FloatType> scaledImg = factory.create(maxDimensions);
        RandomAccess<FloatType> scaledRA = scaledImg.randomAccess();
        Img<FloatType> scaledImg2 = factory.create(tmpDimensions);
        RandomAccess<FloatType> scaledRA2 = scaledImg2.randomAccess();

        int maxval = (int) img.firstElement().getMaxValue();

        double xs, ys, zs;
        for (int z = 0; z <= dstDepth-1; z++) {
            zs = (z - dstCenterZ) / zScale + srcCenterZ;
            for (int y = 0; y <= height-1; y++) {
                for (int x = 0; x <= width-1; x++) {
                    double value = getCubicInterpolatedPixelZ(x, y, zs, width, height, depth, imgRA);
                    scaledRA.setPositionAndGet(x, y, z).setReal(value);
                }
            }
        }

        for (int z = 0; z <= dstDepth-1; z++) {
            for (int y = 0; y <= dstHeight-1; y++) {
                ys = (y-dstCenterY)/yScale + srcCenterY;
                for (int x = 0; x <= width-1; x++) {
                    double value = getCubicInterpolatedPixelY(x, ys, z, width, height, dstDepth, scaledRA);
                    scaledRA2.setPositionAndGet(x, y, z).setReal(value);
                }
            }
        }

        for (int z = 0; z <= dstDepth-1; z++) {
            for (int y = 0; y <= dstHeight-1; y++) {
                for (int x = 0; x <= dstWidth-1; x++) {
                    xs = (x-dstCenterX)/xScale + srcCenterX;
                    double value = getCubicInterpolatedPixelX(xs, y, z, width, dstHeight, dstDepth, scaledRA2);
                    scaledRA.setPositionAndGet(x, y, z).setReal(value);
                }
            }
        }

        Img<T> finalImg = img.factory().create(newDimensions);
        long[] min = new long[]{0, 0, 0}; // Starting coordinates of the rectangle
        long[] max = new long[]{newDimensions[0]-1, newDimensions[1]-1, newDimensions[2]-1}; // Ending coordinates of the rectangle

        // Create a view on the specified interval
        IntervalView<FloatType> srcRegion = Views.interval(scaledImg, min, max);

        Cursor<FloatType> srcCursor = srcRegion.cursor();
        Cursor<T> destCursor = finalImg.cursor();
        while (srcCursor.hasNext() && destCursor.hasNext()) {
            int value = (int)(srcCursor.next().getRealFloat() + 0.5f);
            if (value<0) value = 0;
            if (value>maxval) value = maxval;
            destCursor.next().setInteger(value);
        }

        return finalImg;
    }

}