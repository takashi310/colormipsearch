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

        ImageArray testMIP = new ImageArray(testImage);

        ImageArray maxFilteredImage = ImageOperations.ImageProcessing.create()
                .maxWithDiscPattern(10)
                .applyTo(testMIP)
                .asImageArray()
                ;
        RankFilters maxFilter = new RankFilters();
        maxFilter.rank(testImage.getProcessor(), 10, RankFilters.MAX);

        Assert.assertArrayEquals((int[]) testImage.getProcessor().getPixels(), maxFilteredImage.pixels);
    }

    @Test
    public void maxFilterForBinary8Image() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest.tif", 1);

        ImageArray testMIP = new ImageArray(testImage);

        ImageArray binaryMaxFilteredImage = ImageOperations.ImageProcessing.create()
                .toBinary8(50)
                .maxWithDiscPattern(10)
                .applyTo(testMIP)
                .asImageArray()
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

        ImageArray testMIP = new ImageArray(testImage);

        ImageArray mirroredImage = ImageOperations.ImageProcessing.create()
                .horizontalMirror()
                .applyTo(testMIP)
                .asImageArray()
                ;

        testImage.getProcessor().flipHorizontal();

        Assert.assertArrayEquals((int[]) testImage.getProcessor().getPixels(), mirroredImage.pixels);
    }

    @Test
    public void imageSignal() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest.tif", 1);

        ImageArray testMIP = new ImageArray(testImage);

        ImageArray signalImage = ImageOperations.ImageProcessing.create()
                .toGray16()
                .toSignal()
                .applyTo(testMIP)
                .asImageArray()
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

        ImageArray testMIP = new ImageArray(testImage);

        ImageArray maskedImage = ImageOperations.ImageProcessing.create()
                .mask(250)
                .maxWithDiscPattern(10)
                .applyTo(testMIP)
                .asImageArray()
                ;

        Assert.assertNotNull(maskedImage);
    }

}
