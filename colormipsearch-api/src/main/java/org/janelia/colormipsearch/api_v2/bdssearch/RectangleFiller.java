package org.janelia.colormipsearch.api_v2.bdssearch;

import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.RandomAccess;

public class RectangleFiller {
        // Create a sample RGB image of 100x100 pixels
        //Img<ARGBType> image = ArrayImgs.argbs(100, 100);

        // Define the rectangle's top-left corner, width, and height
        //int startX = 20;
        //int startY = 20;
        //int width = 30;
        //int height = 40;

        // Define a color in ARGB format (e.g., red)
        //int red = 255, green = 0, blue = 0; // Pure red color
        //int argbColor = (255 << 24) | (red << 16) | (green << 8) | blue; // Alpha set to 255 (fully opaque)

        // Fill the rectangle in the image with the specified color
        //fillRectangle(image, startX, startY, width, height, new ARGBType(argbColor));

    private static void fillRectangle(Img<ARGBType> image, int startX, int startY, int width, int height, ARGBType color) {
        RandomAccess<ARGBType> randomAccess = image.randomAccess();

        // Iterate over the rectangle and set the pixel values
        for (int y = startY; y < startY + height; y++) {
            for (int x = startX; x < startX + width; x++) {
                randomAccess.setPosition(x, 0); // Set x-coordinate
                randomAccess.setPosition(y, 1); // Set y-coordinate
                randomAccess.get().set(color);
            }
        }
    }
}