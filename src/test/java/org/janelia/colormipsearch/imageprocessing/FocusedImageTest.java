package org.janelia.colormipsearch.imageprocessing;

import java.util.function.Function;

import ij.IJ;
import ij.ImagePlus;
import ij.io.Opener;
import ij.plugin.filter.RankFilters;
import org.junit.Assert;
import org.junit.Test;

public class FocusedImageTest {

    @Test
    public void maxFilterForRGBImageWithHorizontalMirroring() {

        Function<FocusedImage, Integer> maxFilter = FocusedImage.maxFilter(10);
        Function<FocusedImage, Integer> horizontalMirror = FocusedImage.horizontalMirror();

        for (int i = 0; i < 1; i++) {
            ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest" + (i % 2 + 1) + ".tif", 1);
            ImageArray testMIP = new ImageArray(testImage);
            ImageArray maxFilteredImage = FocusedImage
                    .fromImageArray(testMIP)
                    .extend(maxFilter)
                    .extend(horizontalMirror)
                    .toImageArray();
            IJ.save(new ImagePlus(null, maxFilteredImage.getImageProcessor()), "tt.png");
            RankFilters ijMaxFilter = new RankFilters();
            ijMaxFilter.rank(testImage.getProcessor(), 10, RankFilters.MAX);
            testImage.getProcessor().flipHorizontal();

            Assert.assertArrayEquals((int[]) testImage.getProcessor().getPixels(), maxFilteredImage.pixels);
        }

    }

}
