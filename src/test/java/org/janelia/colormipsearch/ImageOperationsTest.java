package org.janelia.colormipsearch;

import ij.IJ;
import ij.ImagePlus;
import ij.io.Opener;
import ij.plugin.filter.RankFilters;
import ij.process.ImageProcessor;
import org.junit.Assert;
import org.junit.Test;

public class ImageOperationsTest {

    @Test
    public void maxFilterForRGBImage() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest.tif", 1);

        MIPImage testMIP = new MIPImage(new MIPInfo(), testImage);

        MIPImage maxFilteredImage = ImageOperations.ImageProcessing.createFor(testMIP)
                .maxFilter(10)
                .asImage()
                ;
        RankFilters maxFilter = new RankFilters();
        maxFilter.rank(testImage.getProcessor(), 10, RankFilters.MAX);

        Assert.assertArrayEquals((int[]) testImage.getProcessor().getPixels(), maxFilteredImage.pixels);
    }

    @Test
    public void maxFilterForBinary8Image() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest.tif", 1);

        MIPImage testMIP = new MIPImage(new MIPInfo(), testImage);

        MIPImage binaryMaxFilteredImage = ImageOperations.ImageProcessing.createFor(testMIP)
                .toBinary8(50)
                .maxFilter(10)
                .asImage()
                ;

        RankFilters maxFilter = new RankFilters();
        ImageProcessor asByteProcessor = testImage.getProcessor().convertToByte(true);
        maxFilter.rank(asByteProcessor, 10, RankFilters.MAX);

        for (int i = 0; i < asByteProcessor.getPixelCount(); i++) {
            // if the value is > 0 compare with 255 otherwise with 0 since our test image is binary
            Assert.assertEquals(asByteProcessor.get(i) > 0 ? 255 : 0, binaryMaxFilteredImage.get(i));
        }
    }

    @Test
    public void mirrorHorizontally() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest.tif", 1);

        MIPImage testMIP = new MIPImage(new MIPInfo(), testImage);

        MIPImage mirroredImage = ImageOperations.ImageProcessing.createFor(testMIP)
                .mirror()
                .asImage()
                ;

        testImage.getProcessor().flipHorizontal();

        Assert.assertArrayEquals((int[]) testImage.getProcessor().getPixels(), mirroredImage.pixels);
    }

    @Test
    public void imageSignal() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest.tif", 1);

        MIPImage testMIP = new MIPImage(new MIPInfo(), testImage);

        MIPImage signalImage = ImageOperations.ImageProcessing.createFor(testMIP)
                .toGray16()
                .toSignal()
                .asImage()
                ;

        ImageProcessor asShortProcessor = testImage.getProcessor().convertToShortProcessor(true);

        for (int i = 0; i < asShortProcessor.getPixelCount(); i++) {
            // if the value is > 0 compare with 255 otherwise with 0 since our test image is binary
            Assert.assertEquals(asShortProcessor.get(i) > 0 ? 1 : 0, signalImage.get(i));
        }
    }

    @Test
    public void maskRGBImage() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest.tif", 1);

        MIPImage testMIP = new MIPImage(new MIPInfo(), testImage);

        MIPImage maskedImage = ImageOperations.ImageProcessing.createFor(testMIP)
                .maxFilter(10)
                .mask(200)
                .asImage()
                ;

        IJ.save(new ImagePlus(null, maskedImage.getImageProcessor()), "tt.png");
    }

}
