package org.janelia.colormipsearch.api_v2.bdssearch;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.algorithm.morphology.Dilation;
import net.imglib2.algorithm.neighborhood.CenteredRectangleShape;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;

import static org.janelia.colormipsearch.api_v2.bdssearch.ImgUtils.*;

public class DistanceTransform {

    public static Img<?> GenerateDistanceTransform(Img<ARGBType> input, int radius) {
        final long width = input.dimension(0);
        final long height = input.dimension(1);

        Img<UnsignedShortType> input16 = ConvertARGBToGray16(input);
        Img<UnsignedShortType> temp = input16.factory().create(input16);
        MaximumFilter.applyX(input16, temp, radius);
        MaximumFilter.applyY(temp, input16, radius);
        Img<FloatType> dilatedInput32 = ConvertGray16ToFloat32(input16);

        Cursor<FloatType> cursor = dilatedInput32.cursor();
        while (cursor.hasNext()) {
            cursor.fwd();
            float val = cursor.get().get();
            if (val > 1)
                cursor.get().set(0.0f);
            else
                cursor.get().set(Float.MAX_VALUE);
        }
        dt(dilatedInput32);
        Img<?> distanceMap = ConvertFloat32ToGray16(dilatedInput32);

        return distanceMap;
    }

    public static Img<?> GenerateDistanceTransformWithoutDilation(Img<ARGBType> input) {
        Img<FloatType> dilatedInput32 = ConvertARGBToFloat32(input);
        Cursor<FloatType> cursor = dilatedInput32.cursor();
        while (cursor.hasNext()) {
            cursor.fwd();
            float val = cursor.get().get();
            if (val > 1)
                cursor.get().set(0.0f);
            else
                cursor.get().set(Float.MAX_VALUE);
        }

        dt(dilatedInput32);
        Img<?> distanceMap = ConvertFloat32ToGray16(dilatedInput32);

        return distanceMap;
    }

    /* dt of 2d function using squared distance */
    private static void dt(Img<FloatType> im) {
        final int width = (int)im.dimension(0);
        final int height = (int)im.dimension(1);
        final float[] f = new float[Math.max(width,height)];
        final float[] d = new float[Math.max(width,height)];
        final int[] v = new int[Math.max(width,height)];
        final float[] z = new float[Math.max(width,height)+1];
        int n = 0;

        RandomAccess<FloatType> ra = im.randomAccess();

        // transform along columns
        for (int x = 0; x < width; x++) {
            ra.setPosition(x, 0);
            for (int y = 0; y < height; y++) {
                ra.setPosition(y, 1);
                f[y] = ra.get().get();
            }

            int k = 0;
            n = height;
            v[0] = 0;
            z[0] = -Float.MAX_VALUE;
            z[1] = Float.MAX_VALUE;
            for (int q = 1; q <= n-1; q++) {
                float s  = ((f[q]+q*q)-(f[v[k]]+v[k]*v[k]))/(2*q-2*v[k]);
                while (s <= z[k]) {
                    k--;
                    s  = ((f[q]+q*q)-(f[v[k]]+v[k]*v[k]))/(2*q-2*v[k]);
                }
                k++;
                v[k] = q;
                z[k] = s;
                z[k+1] = Float.MAX_VALUE;
            }
            k = 0;
            for (int q = 0; q <= n-1; q++) {
                while (z[k+1] < q)
                    k++;
                d[q] = (q-v[k])*(q-v[k]) + f[v[k]];
            }
            for (int y = 0; y < height; y++) {
                ra.setPosition(y, 1);
                ra.get().set(d[y]);
            }
        }

        // transform along rows
        for (int y = 0; y < height; y++) {
            ra.setPosition(y, 1);
            for (int x = 0; x < width; x++) {
                ra.setPosition(x, 0);
                f[x] = ra.get().get();
            }

            int k = 0;
            n = width;
            v[0] = 0;
            z[0] = -Float.MAX_VALUE;
            z[1] = Float.MAX_VALUE;
            for (int q = 1; q <= n-1; q++) {
                float s  = ((f[q]+q*q)-(f[v[k]]+v[k]*v[k]))/(2*q-2*v[k]);
                while (s <= z[k]) {
                    k--;
                    s  = ((f[q]+q*q)-(f[v[k]]+v[k]*v[k]))/(2*q-2*v[k]);
                }
                k++;
                v[k] = q;
                z[k] = s;
                z[k+1] = Float.MAX_VALUE;;
            }
            k = 0;
            for (int q = 0; q <= n-1; q++) {
                while (z[k+1] < q)
                    k++;
                d[q] = (q-v[k])*(q-v[k]) + f[v[k]];
            }

            for (int x = 0; x < width; x++) {
                ra.setPosition(x, 0);
                ra.get().set(d[x]);
            }
        }

        Cursor<FloatType> cursor = im.cursor();
        while (cursor.hasNext()) {
            cursor.fwd();
            float val = cursor.get().get();
            cursor.get().set((float)Math.sqrt(val));
        }
    }
}
