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
        ArrayList<Integer> pixelValues = new ArrayList<>();
        Cursor<T> cursor = Views.iterable(image).cursor();
        while (cursor.hasNext()) {
            pixelValues.add((int) cursor.next().getRealDouble());
        }
        Collections.sort(pixelValues);

        // Determine the upper intensity threshold
        int upperThreshold = pixelValues.get((int) Math.max(0, upperPixelCount - 1));

        int def_minIntensity = pixelValues.get(0);
        int def_maxIntensity = pixelValues.get(pixelValues.size()-1);

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
                double scaledValue = maxIntensity * (value - minIntensity) / (upperThreshold - minIntensity);
                pixel.setInteger((int) scaledValue);
            }
        }
    }

    public static <T extends IntegerType< T >> void scaleHistogram(Img<T> image, int srcMaxIntensity, int srcMinIntensity, int dstMaxIntensity, int dstMinIntensity) {

        // Convert the upper saturation limit to the number of pixels
        long totalPixels = image.size();
        Cursor<T> cursor = Views.iterable(image).cursor();

        // Stretch the histogram considering the upper saturation
        cursor.reset();
        while (cursor.hasNext()) {
            T pixel = cursor.next();
            int value = pixel.getInteger();
            double scaledValue = (dstMaxIntensity - dstMinIntensity) * (value - srcMinIntensity) / (srcMaxIntensity - srcMinIntensity) + dstMinIntensity;
            if (scaledValue > dstMaxIntensity)
                scaledValue = dstMaxIntensity;
            pixel.setInteger(Math.round(scaledValue));
        }
    }
}