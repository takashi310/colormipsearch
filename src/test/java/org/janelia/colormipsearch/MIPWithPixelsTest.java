package org.janelia.colormipsearch;

import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

import ij.ImagePlus;
import ij.io.Opener;
import ij.plugin.filter.RankFilters;
import ij.process.ImageConverter;
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

    @Test
    public void convertToGray8() throws IOException {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest.tif", 1);

        MIPWithPixels testMIP = new MIPWithPixels(new MIPInfo(), testImage);
        MIPWithPixels testGrayImage = testMIP.asGray8();

        ImagePlus toBeAGrayImage = testMIP.cloneAsImagePlus();
        ImageConverter ic = new ImageConverter(toBeAGrayImage);
        ic.convertToGray8();
        Assert.assertArrayEquals((byte[]) toBeAGrayImage.getProcessor().getPixels(), (byte[]) testGrayImage.cloneAsImagePlus().getProcessor().getPixels());

//        ImageIO.write(testGrayImage.cloneAsImagePlus().getBufferedImage(), "png", new File("src/test/resources/colormipsearch/tt.png"));
    }
}
