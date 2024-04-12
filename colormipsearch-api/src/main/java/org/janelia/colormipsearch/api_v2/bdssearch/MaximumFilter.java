package org.janelia.colormipsearch.api_v2.bdssearch;

import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;

import static org.janelia.colormipsearch.api_v2.bdssearch.ImgUtils.saveAsTiff;
import static org.janelia.colormipsearch.api_v2.bdssearch.ImgUtils.saveImageAsPNG;

public class MaximumFilter {

    public static <T extends IntegerType<T>> void applyX(Img<T> input, Img<T> output, int radius) {
        long width = input.dimension(0);
        long height = input.dimension(1);
        long depth = input.dimension(2);

        RandomAccess<T> outputRA = output.randomAccess();
        RandomAccess<T> inputRA = input.randomAccess();

        for (int z = 0; z < depth; z++) {
            if (input.numDimensions() > 2) {
                inputRA.setPosition(z, 2);
                outputRA.setPosition(z, 2);
            }
            for (int y = 0; y < height; y++) {
                inputRA.setPosition(y, 1);
                outputRA.setPosition(y, 1);
                for (int x = 0; x < width; x++) {
                    int maxIntensity = 0;
                    for (int r = -radius; r <= radius; r++) {
                        int xx = x + r;
                        if (xx >= 0 && xx < width) {
                            inputRA.setPosition(xx, 0);
                            int val = inputRA.get().getInteger();
                            if (val > maxIntensity) maxIntensity = val;
                        }
                    }
                    outputRA.setPosition(x, 0);
                    outputRA.get().setInteger(maxIntensity);
                }
            }
        }
    }

    public static <T extends IntegerType<T>> void applyY(Img<T> input, Img<T> output, int radius) {
        long width = input.dimension(0);
        long height = input.dimension(1);
        long depth = input.dimension(2);

        RandomAccess<T> outputRA = output.randomAccess();
        RandomAccess<T> inputRA = input.randomAccess();

        for (int z = 0; z < depth; z++) {
            if (input.numDimensions() > 2) {
                inputRA.setPosition(z, 2);
                outputRA.setPosition(z, 2);
            }
            for (int x = 0; x < width; x++) {
                inputRA.setPosition(x, 0);
                outputRA.setPosition(x, 0);
                for (int y = 0; y < height; y++) {
                    int maxIntensity = 0;
                    for (int r = -radius; r <= radius; r++) {
                        int yy = y + r;
                        if (yy >= 0 && yy < height) {
                            inputRA.setPosition(yy, 1);
                            int val = inputRA.get().getInteger();
                            if (val > maxIntensity) maxIntensity = val;
                        }
                    }
                    outputRA.setPosition(y, 1);
                    outputRA.get().setInteger(maxIntensity);
                }
            }
        }
    }

    public static <T extends IntegerType<T>> void applyZ(Img<T> input, Img<T> output, int radius) {
        long width = input.dimension(0);
        long height = input.dimension(1);
        long depth = input.dimension(2);

        RandomAccess<T> outputRA = output.randomAccess();
        RandomAccess<T> inputRA = input.randomAccess();

        for (int x = 0; x < width; x++) {
            inputRA.setPosition(x, 0);
            outputRA.setPosition(x, 0);
            for (int y = 0; y < height; y++) {
                inputRA.setPosition(y, 1);
                outputRA.setPosition(y, 1);
                for (int z = 0; z < depth; z++) {
                    int maxIntensity = 0;
                    for (int r = -radius; r <= radius; r++) {
                        int zz = z + r;
                        if (zz >= 0 && zz < depth) {
                            inputRA.setPosition(zz, 2);
                            int val = inputRA.get().getInteger();
                            if (val > maxIntensity) maxIntensity = val;
                        }
                    }
                    outputRA.setPosition(z, 2);
                    outputRA.get().setInteger(maxIntensity);
                }
            }
        }
    }

    public static void applyX_ARGB(Img<ARGBType> input, Img<ARGBType> output, int radius) {
        long width = input.dimension(0);
        long height = input.dimension(1);
        long depth = input.dimension(2);

        RandomAccess<ARGBType> outputRA = output.randomAccess();
        RandomAccess<ARGBType> inputRA = input.randomAccess();

        for (int z = 0; z < depth; z++) {
            if (input.numDimensions() > 2) {
                inputRA.setPosition(z, 2);
                outputRA.setPosition(z, 2);
            }
            for (int y = 0; y < height; y++) {
                inputRA.setPosition(y, 1);
                outputRA.setPosition(y, 1);
                for (int x = 0; x < width; x++) {
                    int maxIntensityR = 0;
                    int maxIntensityG = 0;
                    int maxIntensityB = 0;
                    for (int r = -radius; r <= radius; r++) {
                        int xx = x + r;
                        if (xx >= 0 && xx < width) {
                            inputRA.setPosition(xx, 0);
                            int val = inputRA.get().get();
                            int rv = (val >> 16) & 0xFF;
                            int gv = (val >> 8) & 0xFF;
                            int bv = val & 0xFF;
                            if (rv > maxIntensityR) maxIntensityR = rv;
                            if (gv > maxIntensityG) maxIntensityG = gv;
                            if (bv > maxIntensityB) maxIntensityB = bv;
                        }
                    }
                    outputRA.setPosition(x, 0);
                    outputRA.get().set(0xFF000000 | (maxIntensityR << 16) | (maxIntensityG << 8) | maxIntensityB);
                }
            }
        }
    }

    public static void applyY_ARGB(Img<ARGBType> input, Img<ARGBType> output, int radius) {
        long width = input.dimension(0);
        long height = input.dimension(1);
        long depth = input.dimension(2);

        RandomAccess<ARGBType> outputRA = output.randomAccess();
        RandomAccess<ARGBType> inputRA = input.randomAccess();

        for (int z = 0; z < depth; z++) {
            if (input.numDimensions() > 2) {
                inputRA.setPosition(z, 2);
                outputRA.setPosition(z, 2);
            }
            for (int x = 0; x < width; x++) {
                inputRA.setPosition(x, 0);
                outputRA.setPosition(x, 0);
                for (int y = 0; y < height; y++) {
                    int maxIntensityR = 0;
                    int maxIntensityG = 0;
                    int maxIntensityB = 0;
                    for (int r = -radius; r <= radius; r++) {
                        int yy = y + r;
                        if (yy >= 0 && yy < height) {
                            inputRA.setPosition(yy, 1);
                            int val = inputRA.get().get();
                            int rv = (val >> 16) & 0xFF;
                            int gv = (val >> 8) & 0xFF;
                            int bv = val & 0xFF;
                            if (rv > maxIntensityR) maxIntensityR = rv;
                            if (gv > maxIntensityG) maxIntensityG = gv;
                            if (bv > maxIntensityB) maxIntensityB = bv;
                        }
                    }
                    outputRA.setPosition(y, 1);
                    outputRA.get().set(0xFF000000 | (maxIntensityR << 16) | (maxIntensityG << 8) | maxIntensityB);
                }
            }
        }
    }

    public static void applyZ_ARGB(Img<ARGBType> input, Img<ARGBType> output, int radius) {
        long width = input.dimension(0);
        long height = input.dimension(1);
        long depth = input.dimension(2);

        RandomAccess<ARGBType> outputRA = output.randomAccess();
        RandomAccess<ARGBType> inputRA = input.randomAccess();

        for (int x = 0; x < width; x++) {
            inputRA.setPosition(x, 0);
            outputRA.setPosition(x, 0);
            for (int y = 0; y < height; y++) {
                inputRA.setPosition(y, 1);
                outputRA.setPosition(y, 1);
                for (int z = 0; z < depth; z++) {
                    int maxIntensityR = 0;
                    int maxIntensityG = 0;
                    int maxIntensityB = 0;
                    for (int r = -radius; r <= radius; r++) {
                        int zz = z + r;
                        if (zz >= 0 && zz < depth) {
                            inputRA.setPosition(zz, 2);
                            int val = inputRA.get().get();
                            int rv = (val >> 16) & 0xFF;
                            int gv = (val >> 8) & 0xFF;
                            int bv = val & 0xFF;
                            if (rv > maxIntensityR) maxIntensityR = rv;
                            if (gv > maxIntensityG) maxIntensityG = gv;
                            if (bv > maxIntensityB) maxIntensityB = bv;
                        }
                    }
                    outputRA.setPosition(z, 2);
                    outputRA.get().set(0xFF000000 | (maxIntensityR << 16) | (maxIntensityG << 8) | maxIntensityB);
                }
            }
        }
    }

    public static <T extends IntegerType<T>> Img<T> apply3D(Img<T> input, int radiusXY, int radiusZ) {
        Img<T> temp = input.factory().create(input);
        Img<T> output = input.factory().create(input);
        MaximumFilter.applyX(input, output, radiusXY);
        MaximumFilter.applyY(output, temp, radiusXY);
        MaximumFilter.applyZ(temp, output, radiusZ);

        return output;
    }

    public static <T extends IntegerType<T>> Img<T> apply2D(Img<T> input, int radius) {
        Img<T> temp = input.factory().create(input);
        Img<T> output = input.factory().create(input);
        MaximumFilter.applyX(input, temp, radius);
        MaximumFilter.applyY(temp, output, radius);

        return output;
    }

    public static Img<ARGBType> apply2D_ARGB(Img<ARGBType> input, int radius, int threshold) {
        Img<ARGBType> temp = input.factory().create(input);
        Img<ARGBType> output = input.factory().create(input);

        Cursor<ARGBType> srcCursor = input.cursor();
        Cursor<ARGBType> dstCursor = output.cursor();
        while(srcCursor.hasNext() && dstCursor.hasNext()) {
            srcCursor.fwd();
            dstCursor.fwd();
            int val = srcCursor.get().get();
            if ((val & 0x00FFFFFF) != 0) {
                int r = (val >> 16) & 0xFF;
                int g = (val >> 8) & 0xFF;
                int b = val & 0xFF;
                if (r <= threshold && g <= threshold && b <= threshold)
                    val = 0xFF000000;
                dstCursor.get().set(val);
            }
        }

        MaximumFilter.applyX_ARGB(output, temp, radius);
        MaximumFilter.applyY_ARGB(temp, output, radius);

        return output;
    }
}
