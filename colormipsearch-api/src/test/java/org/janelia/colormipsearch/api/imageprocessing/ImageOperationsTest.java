package org.janelia.colormipsearch.api.imageprocessing;

import ij.ImagePlus;
import ij.io.Opener;
import ij.plugin.filter.RankFilters;
import ij.process.ImageConverter;
import ij.process.ImageProcessor;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ImageOperationsTest {

    @Test
    public void overExpressesMaskExpression() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/api/imageprocessing/1281324958-DNp11-RT_18U_FL.tif", 1);
        ImageArray<?> testImageArray = ImageArrayUtils.fromImagePlus(testImage);
        LImage testQueryImage = LImageUtils.create(testImageArray).mapi(ImageTransformation.clearRegion(ImageTransformation.IS_LABEL_REGION));
        LImage maskForRegionsWithTooMuchExpression = LImageUtils.combine2(
                testQueryImage.mapi(ImageTransformation.maxFilter(60)),
                testQueryImage.mapi(ImageTransformation.maxFilter(20)),
                (p1, p2) -> p2 != -16777216 && p2 != 0 ? -16777216 : p1 // mask pixels from the 60x image if they are present in the 20x image
        );
        ImageArray<?> res = maskForRegionsWithTooMuchExpression.map(ColorTransformation.toGray16WithNoGammaCorrection()).map(ColorTransformation.toSignalRegions(0)).reduce().toImageArray();
        Integer nonZeroPxs = LImageUtils.create(res).fold(0, (p, s) -> p == 0 ? s : s+1);
        assertTrue(nonZeroPxs > 0);
    }

    @Test
    public void maxFilterForRGBImage() {
        ImageProcessing maxFilterProcessing = ImageProcessing.create()
                .maxFilter(10);

        for (int i = 0; i < 5; i++) {
            ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/api/imageprocessing/minmaxTest" + (i % 2 + 1) + ".tif", 1);
            ImageArray<?> testMIP = ImageArrayUtils.fromImagePlus(testImage);
            ImageArray<?> maxFilteredImage = maxFilterProcessing
                    .applyTo(testMIP)
                    .toImageArray();
            RankFilters maxFilter = new RankFilters();
            maxFilter.rank(testImage.getProcessor(), 10, RankFilters.MAX);

            for (int j = 0; j < testImage.getProcessor().getPixelCount(); j++) {
                Assert.assertEquals((testImage.getProcessor().get(j) & 0x00FFFFFF), maxFilteredImage.get(j) & 0x00FFFFFF);
            }
        }
    }

    @Test
    public void maxFilterThenHorizontalMirroringForRGBImage() {
        ImageProcessing maxFilterProcessing = ImageProcessing.create()
                .thenExtend(ImageTransformation.maxFilter(10))
                .thenExtend(ImageTransformation.horizontalMirror());

        for (int i = 1; i < 6; i++) {
            ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/api/imageprocessing/minmaxTest" + (i % 2 + 1) + ".tif", 1);
            ImageArray<?> testMIP = ImageArrayUtils.fromImagePlus(testImage);
            ImageArray<?> maxFilteredImage = maxFilterProcessing
                    .applyTo(testMIP)
                    .toImageArray();
            RankFilters maxFilter = new RankFilters();
            maxFilter.rank(testImage.getProcessor(), 10, RankFilters.MAX);
            testImage.getProcessor().flipHorizontal();

            for (int j = 0; j < testImage.getProcessor().getPixelCount(); j++) {
                Assert.assertEquals((testImage.getProcessor().get(j) & 0x00FFFFFF), maxFilteredImage.get(j) & 0x00FFFFFF);
            }
        }
    }

    @Test
    public void horizontalMirrorThenMaxFilterForRGBImage() {
        ImageProcessing maxFilterProcessing = ImageProcessing.create()
                .thenExtend(ImageTransformation.horizontalMirror())
                .thenExtend(ImageTransformation.maxFilter(10));

        for (int i = 0; i < 5; i++) {
            ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/api/imageprocessing/minmaxTest" + (i % 2 + 1) + ".tif", 1);
            ImageArray<?> testMIP = ImageArrayUtils.fromImagePlus(testImage);
            ImageArray<?> maxFilteredImage = maxFilterProcessing
                    .applyTo(testMIP).toImageArray();
            RankFilters maxFilter = new RankFilters();
            maxFilter.rank(testImage.getProcessor(), 10, RankFilters.MAX);
            testImage.getProcessor().flipHorizontal();

            for (int j = 0; j < testImage.getProcessor().getPixelCount(); j++) {
                Assert.assertEquals((testImage.getProcessor().get(j) & 0x00FFFFFF), maxFilteredImage.get(j) & 0x00FFFFFF);
            }
        }
    }

    @Test
    public void maxFilterForBinary8Image() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/api/imageprocessing/minmaxTest1.tif", 1);

        ImageArray<?> testMIP = ImageArrayUtils.fromImagePlus(testImage);

        ImageArray<?> binaryMaxFilteredImage = ImageProcessing.create()
                .toBinary8(50)
                .maxFilter(10)
                .applyTo(testMIP)
                .toImageArray();

        RankFilters maxFilter = new RankFilters();
        ImageProcessor asByteProcessor = testImage.getProcessor().convertToByte(true);
        maxFilter.rank(asByteProcessor, 10, RankFilters.MAX);

        for (int i = 0; i < asByteProcessor.getPixelCount(); i++) {
            // if the value is > 0 compare with 255 otherwise with 0 since our test image is binary
            Assert.assertEquals(asByteProcessor.get(i) > 0 ? 255 : 0, binaryMaxFilteredImage.get(i));
        }
    }

    @Test
    public void convertToGray8WithNoGammaCorrection() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/api/imageprocessing/minmaxTest1.tif", 1);

        ImageArray<?> testMIP = ImageArrayUtils.fromImagePlus(testImage);

        ImageConverter ic = new ImageConverter(testImage);
        ic.convertToGray8();

        ImageArray<?> grayImage = LImageUtils.create(testMIP).map(ColorTransformation.toGray8WithNoGammaCorrection()).toImageArray();
        ImageProcessor convertedImageProcessor = testImage.getProcessor();

        for (int i = 0; i < convertedImageProcessor.getPixelCount(); i++) {
            // if the value is > 0 compare with 255 otherwise with 0 since our test image is binary
            Assert.assertEquals(convertedImageProcessor.get(i), grayImage.get(i));
        }
    }

    @Test
    public void convertToGray16WithNoGammaCorrection() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/api/imageprocessing/minmaxTest1.tif", 1);

        ImageArray<?> testMIP = ImageArrayUtils.fromImagePlus(testImage);

        ImageConverter ic = new ImageConverter(testImage);
        ic.convertToGray16();

        ImageArray<?> grayImage = LImageUtils.create(testMIP).map(ColorTransformation.toGray16WithNoGammaCorrection()).toImageArray();
        ImageProcessor convertedImageProcessor = testImage.getProcessor();

        for (int i = 0; i < convertedImageProcessor.getPixelCount(); i++) {
            // if the value is > 0 compare with 255 otherwise with 0 since our test image is binary
            Assert.assertEquals(convertedImageProcessor.get(i), grayImage.get(i));
        }
    }

    @Test
    public void mirrorHorizontally() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/api/imageprocessing/minmaxTest1.tif", 1);

        ImageArray<?> testMIP = ImageArrayUtils.fromImagePlus(testImage);

        ImageArray<?> mirroredImage = ImageProcessing.create()
                .horizontalMirror()
                .applyTo(testMIP)
                .toImageArray();

        testImage.getProcessor().flipHorizontal();

        for (int i = 0; i < testImage.getProcessor().getPixelCount(); i++) {
            // if the value is > 0 compare with 255 otherwise with 0 since our test image is binary
            Assert.assertEquals(testImage.getProcessor().get(i) & 0x00FFFFFF, mirroredImage.get(i) & 0x00FFFFFF);
        }
    }

    @Test
    public void imageSignal() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/api/imageprocessing/minmaxTest1.tif", 1);

        ImageArray<?> testMIP = ImageArrayUtils.fromImagePlus(testImage);

        ImageArray<?> signalImage = ImageProcessing.create()
                .toGray16()
                .toSignalRegions(0)
                .applyTo(testMIP)
                .toImageArray();

        ImageProcessor asShortProcessor = testImage.getProcessor().convertToShortProcessor(true);

        for (int i = 0; i < asShortProcessor.getPixelCount(); i++) {
            // if the value is > 0 compare with 255 otherwise with 0 since our test image is binary
            Assert.assertEquals(asShortProcessor.get(i) > 0 ? 1 : 0, signalImage.get(i) & 0x00FFFFFF);
        }
    }

    @Test
    public void maskRGBImage() {
        ImagePlus testImage = new Opener().openTiff("src/test/resources/colormipsearch/api/imageprocessing/minmaxTest1.tif", 1);

        ImageArray<?> testMIP = ImageArrayUtils.fromImagePlus(testImage);

        ImageArray<?> maskedImage = ImageProcessing.create()
                .mask(250)
                .maxFilter(10)
                .applyTo(testMIP)
                .toImageArray();

        Assert.assertNotNull(maskedImage);
    }

}
