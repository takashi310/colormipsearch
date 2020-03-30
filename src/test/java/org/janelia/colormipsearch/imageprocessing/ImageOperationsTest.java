package org.janelia.colormipsearch.imageprocessing;

import ij.ImagePlus;
import ij.io.Opener;
import ij.plugin.filter.RankFilters;
import ij.process.ImageProcessor;
import org.junit.Assert;
import org.junit.Test;

public class ImageOperationsTest {

    @Test
    public void maxFilterForRGBImage() {

        ImageProcessing maxFilterProcessing = ImageProcessing.create()
                .maxWithDiscPattern(10);

        for (int i = 0; i < 5; i++) {
            ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest" + (i % 2 + 1) + ".tif", 1);
            ImageArray testMIP = new ImageArray(testImage);
            ImageArray maxFilteredImage = maxFilterProcessing
                    .applyTo(testMIP).asImageArray();
            RankFilters maxFilter = new RankFilters();
            maxFilter.rank(testImage.getProcessor(), 10, RankFilters.MAX);

            Assert.assertArrayEquals((int[]) testImage.getProcessor().getPixels(), maxFilteredImage.pixels);
        }

    }

    @Test
    public void maxFilterForBinary8Image() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest1.tif", 1);

        ImageArray testMIP = new ImageArray(testImage);

        ImageArray binaryMaxFilteredImage = ImageProcessing.create()
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
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest1.tif", 1);

        ImageArray testMIP = new ImageArray(testImage);

        ImageArray mirroredImage = ImageProcessing.create()
                .horizontalMirror()
                .applyTo(testMIP)
                .asImageArray()
                ;

        testImage.getProcessor().flipHorizontal();

        Assert.assertArrayEquals((int[]) testImage.getProcessor().getPixels(), mirroredImage.pixels);
    }

    @Test
    public void imageSignal() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest1.tif", 1);

        ImageArray testMIP = new ImageArray(testImage);

        ImageArray signalImage = ImageProcessing.create()
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
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest1.tif", 1);

        ImageArray testMIP = new ImageArray(testImage);

        ImageArray maskedImage = ImageProcessing.create()
                .mask(250)
                .maxWithDiscPattern(10)
                .applyTo(testMIP)
                .asImageArray()
                ;

        Assert.assertNotNull(maskedImage);
    }

}
