package org.janelia.colormipsearch.api_v2.bdssearch;

import net.imglib2.Cursor;
import net.imglib2.img.Img;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.view.Views;
import java.util.ArrayList;
import java.util.Collections;

public class ContrastEnhancer {

    // Simplified method to stretch the histogram of an image
    public static <T extends IntegerType< T >> void stretchHistogram(Img<T> image, double saturated, int maxIntensity, int minIntensity) {

        // Convert the upper saturation limit to the number of pixels
        long totalPixels = image.size();
        long upperPixelCount = (long) (totalPixels * (100.0 - saturated * 0.5) / 100.0);

        // Collect pixel values
        int[] bins = new int[65536];
        Cursor<T> cursor = Views.iterable(image).cursor();
        while (cursor.hasNext()) {
            int val = cursor.next().getInteger();
            bins[val] = bins[val] + 1;
        }

        // Determine the upper intensity threshold
        int def_minIntensity = -1;
        int def_maxIntensity = 0;
        int upperThreshold = 0;
        int count = 0;
        for (int i = 0; i < 65535; i++) {
            int bin = bins[i];
            count += bin;
            if (count >= upperPixelCount && upperThreshold == 0) {
                upperThreshold = i;
            }
            if (bin > 0) {
                if (def_minIntensity == 0)
                    def_minIntensity = i;
                def_maxIntensity = i;
            }
        }
        if (def_minIntensity < 0)
            def_minIntensity = 0;

        if (maxIntensity < 0)
            maxIntensity = def_maxIntensity;
        if (minIntensity < 0)
            minIntensity = def_minIntensity;

        if (upperThreshold <= minIntensity)
            return;

        // Stretch the histogram considering the upper saturation
        cursor.reset();
        while (cursor.hasNext()) {
            T pixel = cursor.next();
            int value = pixel.getInteger();
            if (value >= upperThreshold) {
                pixel.setInteger(maxIntensity);
            } else {
                double scaledValue = (double)(maxIntensity * (value - minIntensity)) / (double)(upperThreshold - minIntensity);
                pixel.setInteger((int) scaledValue);
            }
        }
    }

    public static <T extends IntegerType< T >> void scaleHistogramRight(Img<T> image, int srcMaxIntensity, int dstMaxIntensity) {
        if (srcMaxIntensity == 0)
            return;
        Cursor<T> cursor = image.cursor();
        while (cursor.hasNext()) {
            T pixel = cursor.next();
            int value = pixel.getInteger();
            if (value > 0) {
                double scaledValue = (double)dstMaxIntensity * value / srcMaxIntensity;
                if (scaledValue > dstMaxIntensity)
                    scaledValue = dstMaxIntensity;
                pixel.setInteger(Math.round(scaledValue));
            }
        }
    }
}