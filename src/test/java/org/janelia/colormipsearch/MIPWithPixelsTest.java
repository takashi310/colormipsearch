package org.janelia.colormipsearch;

import java.util.Arrays;

import ij.ImagePlus;
import ij.io.Opener;
import ij.plugin.filter.RankFilters;
import ij.process.ByteProcessor;
import ij.process.ColorProcessor;
import ij.process.ImageProcessor;
import ij.process.ShortProcessor;
import org.junit.Assert;
import org.junit.Test;

public class MIPWithPixelsTest {

    @Test
    public void maxFilterForRGBImage() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest.tif", 1);

        MIPWithPixels testMIP = new MIPWithPixels(new MIPInfo(), testImage);
        Assert.assertArrayEquals(testMIP.pixels, (int[]) testImage.getProcessor().getPixels());

        ImageTransformer maxFilterTransformer = ImageTransformer.createFor(testMIP)
                .maxFilter(10)
                ;

        // test that now the image is different from the original
        boolean differ = false;
        for (int i = 0; i < testMIP.pixels.length; i++) {
            if (testMIP.get(i) != testImage.getProcessor().get(i)) {
                differ = true;
                break;
            }
        }
        Assert.assertTrue(differ);

        // apply FIJI rank filter to the original and test that the images are equal now
        RankFilters maxFilter = new RankFilters();
        maxFilter.rank(testImage.getProcessor(), 10, RankFilters.MAX);
        Assert.assertArrayEquals((int[]) testImage.getProcessor().getPixels(), testMIP.pixels);
    }

    @Test
    public void maxFilterForBinary8Image() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest.tif", 1);

        MIPWithPixels testMIP = new MIPWithPixels(new MIPInfo(), testImage);

        ImageTransformer binaryMaxFilterTransformer = ImageTransformer.createFor(testMIP)
                .toBinary8(100)
                .maxFilter(10)
                ;

        RankFilters maxFilter = new RankFilters();
        ImageProcessor asByteProcessor = testImage.getProcessor().convertToByte(true);
        maxFilter.rank(asByteProcessor, 10, RankFilters.MAX);

        for (int i = 0; i < asByteProcessor.getPixelCount(); i++) {
            // if the value is > 0 compare with 255 otherwise with 0 since our test image is binary
            Assert.assertEquals(asByteProcessor.get(i) > 0 ? 255 : 0, binaryMaxFilterTransformer.getImage().get(i));
        }
    }

    @Test
    public void maxFilterForBinary16Image() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest.tif", 1);

        MIPWithPixels testMIP = new MIPWithPixels(new MIPInfo(), testImage);

        ImageTransformer binaryMaxFilterTransformer = ImageTransformer.createFor(testMIP)
                .toBinary16(100)
                .maxFilter(10)
                ;
    }

    @Test
    public void convertToGray8() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest.tif", 1);

        MIPWithPixels testMIP = new MIPWithPixels(new MIPInfo(), testImage);

        ImageTransformer toGray8Transformer = ImageTransformer.createFor(testMIP)
                .toGray8()
                ;

        ImagePlus ij = cloneAsImagePlus(toGray8Transformer.getImage());
        Assert.assertTrue(ij.getType() == ImagePlus.GRAY8);
    }

    @Test
    public void convertToGray16() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest.tif", 1);

        MIPWithPixels testMIP = new MIPWithPixels(new MIPInfo(), testImage);

        ImageTransformer toGray8Transformer = ImageTransformer.createFor(testMIP)
                .toGray16()
                ;

        ImagePlus ij = cloneAsImagePlus(toGray8Transformer.getImage());
        Assert.assertTrue(ij.getType() == ImagePlus.GRAY16);
    }

    @Test
    public void mirrorHorizontally() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/minmaxTest.tif", 1);

        MIPWithPixels testMIP = new MIPWithPixels(new MIPInfo(), testImage);

        testImage.getProcessor().flipHorizontal();

        ImageTransformer horizontalMirrorTransformer = ImageTransformer.createFor(testMIP)
                .applyImageTransformation(Operations.ImageTransformation.horizontalMirrorTransformation())
                ;

        Assert.assertArrayEquals((int[]) testImage.getProcessor().getPixels(), horizontalMirrorTransformer.getImage().pixels);
    }

    private ImagePlus cloneAsImagePlus(MIPWithPixels mip) {
        switch (mip.type) {
            case GRAY8:
                byte[] byteImageBuffer = new byte[mip.pixels.length];
                for (int i = 0; i < mip.pixels.length; i++) {
                    byteImageBuffer[i] = (byte) (mip.pixels[i] & 0xFF);
                }
                return new ImagePlus(null, new ByteProcessor(mip.width, mip.height, byteImageBuffer));
            case GRAY16:
                short[] shortImageBuffer = new short[mip.pixels.length];
                for (int i = 0; i < mip.pixels.length; i++) {
                    shortImageBuffer[i] = (short) (mip.pixels[i] & 0xFFFF);
                }
                return new ImagePlus(null, new ShortProcessor(mip.width, mip.height, shortImageBuffer, null));
            default:
                return new ImagePlus(null, new ColorProcessor(mip.width, mip.height, Arrays.copyOf(mip.pixels, mip.pixels.length)));
        }
    }

}
