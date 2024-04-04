package org.janelia.colormipsearch.api_v2.bdssearch;

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

public class DistanceTransform {
    private static short argbToGray16(int argb) {
        if ((argb & 0x00FFFFFF) == 0) {
            return 0;
        } else {
            int r = (argb >> 16) & 0xFF;
            int g = (argb >> 8) & 0xFF;
            int b = (argb & 0xFF);

            double rw = 1 / 3.;
            double gw = 1 / 3.;
            double bw = 1 / 3.;

            return (short) (r*rw + g*gw + b*bw + 0.5);
        }
    }

    private static Img<UnsignedShortType> ConvertARGBToGray16(Img<ARGBType> input) {
        long w = input.dimension(0);
        long h = input.dimension(1);

        ArrayImgFactory<UnsignedShortType> factory = new ArrayImgFactory<>();
        Img<UnsignedShortType> result = factory.create(w, h);

        Cursor<ARGBType> srcCursor = input.cursor();
        Cursor<UnsignedShortType> dstCursor = result.cursor();
        while (srcCursor.hasNext() && dstCursor.hasNext()) {
            dstCursor.next().set(argbToGray16(srcCursor.next().get()));
        }
        return result;
    }

    private static Img<FloatType> ConvertGray16ToFloat32(Img<UnsignedShortType> input) {
        long w = input.dimension(0);
        long h = input.dimension(1);

        ArrayImgFactory<FloatType> factory = new ArrayImgFactory<>();
        Img<FloatType> result = factory.create(w, h);

        Cursor<UnsignedShortType> srcCursor = input.cursor();
        Cursor<FloatType> dstCursor = result.cursor();
        while (srcCursor.hasNext() && dstCursor.hasNext()) {
            dstCursor.next().set((float)srcCursor.next().get());
        }
        return result;
    }

    private static Img<UnsignedShortType> ConvertFloat32ToGray16(Img<FloatType> input) {
        long w = input.dimension(0);
        long h = input.dimension(1);

        ArrayImgFactory<UnsignedShortType> factory = new ArrayImgFactory<>();
        Img<UnsignedShortType> result = factory.create(w, h);

        Cursor<FloatType> srcCursor = input.cursor();
        Cursor<UnsignedShortType> dstCursor = result.cursor();
        while (srcCursor.hasNext() && dstCursor.hasNext()) {
            dstCursor.next().set((short)srcCursor.next().get());
        }
        return result;
    }

    public static Img<?> GenerateDistanceTransform(Img<ARGBType> input, int radius) {
        final long width = input.dimension(0);
        final long height = input.dimension(1);

        Img<UnsignedShortType> input16 = ConvertARGBToGray16(input);

        int[] span_max = {radius, radius, radius};
        CenteredRectangleShape neighborhood = new CenteredRectangleShape(span_max, true); // true for including center
        Img<UnsignedShortType> dilatedInput16 = Dilation.dilate(input16, neighborhood, 1);
        Img<FloatType> dilatedInput32 = ConvertGray16ToFloat32(dilatedInput16);
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
            for (int y = 0; y < height; y++) {
                f[y] = ra.setPositionAndGet(x, y).get();
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
                ra.setPositionAndGet(x, y).set(d[y]);
            }
        }

        // transform along rows
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                f[x] = ra.setPositionAndGet(x, y).get();
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
                ra.setPositionAndGet(x, y).set(d[x]);
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
