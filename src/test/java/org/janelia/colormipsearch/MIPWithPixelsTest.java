package org.janelia.colormipsearch;

import java.io.IOException;

import ij.ImagePlus;
import ij.io.Opener;
import ij.plugin.filter.RankFilters;
import org.junit.Assert;
import org.junit.Test;

public class MIPWithPixelsTest {

    @Test
    public void maxFilterForRGBImage() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest.tif", 1);

        MIPWithPixels testMIP = new MIPWithPixels(new MIPInfo(), testImage);
        MIPWithPixels maxFilteredTestMIP = testMIP.max2DFilter(10);

        Assert.assertNotEquals(maxFilteredTestMIP.pixels, testImage.getProcessor().getPixels());

        RankFilters maxFilter = new RankFilters();

        maxFilter.rank(testImage.getProcessor(), 10, RankFilters.MAX);
        Assert.assertArrayEquals((int[]) testImage.getProcessor().getPixels(), maxFilteredTestMIP.pixels);
    }
}
