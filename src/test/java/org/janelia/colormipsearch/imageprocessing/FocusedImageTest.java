package org.janelia.colormipsearch.imageprocessing;

import java.util.function.Function;

import ij.ImagePlus;
import ij.io.Opener;
import ij.plugin.filter.RankFilters;
import org.junit.Assert;
import org.junit.Test;

public class FocusedImageTest {

    @Test
    public void maxFilterThenHorizontalMirrorForRGBImage() {

        Function<FocusedImage, Integer> maxFilter = FocusedImage.maxFilter(10);
        Function<FocusedImage, Integer> horizontalMirror = FocusedImage.horizontalMirror();

        for (int i = 0; i < 2; i++) {
            ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest" + (i % 2 + 1) + ".tif", 1);
            ImageArray testMIP = new ImageArray(testImage);
            ImageArray maxFilteredImage = FocusedImage
                    .fromImageArray(testMIP)
//                    .extend(horizontalMirror)
                    .extend(maxFilter)
                    .toImageArray();
            RankFilters ijMaxFilter = new RankFilters();
            ijMaxFilter.rank(testImage.getProcessor(), 10, RankFilters.MAX);
//            testImage.getProcessor().flipHorizontal();

            Assert.assertArrayEquals((int[]) testImage.getProcessor().getPixels(), maxFilteredImage.pixels);
        }

    }

}
