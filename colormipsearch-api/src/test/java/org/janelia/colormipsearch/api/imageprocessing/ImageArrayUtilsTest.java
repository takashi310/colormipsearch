package org.janelia.colormipsearch.api.imageprocessing;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

import ij.IJ;
import ij.ImagePlus;
import ij.io.Opener;
import ij.plugin.filter.RankFilters;
import ij.process.ImageProcessor;
import org.junit.Assert;
import org.junit.Test;

public class ImageArrayUtilsTest {

    @Test
    public void readImageRangeForPackBits() throws Exception {
        ImageProcessing maxFilterProcessing = ImageProcessing.create()
                .maxFilter(10);

        for (int i = 0; i < 1; i++) {
            File testFile = new File("src/test/resources/colormipsearch/api/imageprocessing/compressed_pack" + (i % 2 + 1) + ".tif");
            ImagePlus testImage = new Opener().openTiff(testFile.getAbsolutePath(), 1);
            int[] testImageBoundaries = getImageBoundaries(testImage.getProcessor());
            ImageArray<?> testImageArray = ImageArrayUtils.readImageArrayRange("test", testFile.getName(), openFile(testFile), testImageBoundaries[0], testImageBoundaries[1]);
            IJ.save(new ImagePlus(null, ImageArrayUtils.toImageProcessor(testImageArray)), "tt.png");
        }
    }

    @Test
    public void readImageRangeForOtherCompression() throws Exception {
        ImageProcessing maxFilterProcessing = ImageProcessing.create()
                .maxFilter(10);

        for (int i = 0; i < 1; i++) {
            File testFile = new File("src/test/resources/colormipsearch/api/imageprocessing/compressed_lzw" + (i % 2 + 1) + ".tif");
            ImagePlus testImage = new Opener().openTiff(testFile.getAbsolutePath(), 1);
            int[] testImageBoundaries = getImageBoundaries(testImage.getProcessor());
            ImageArray<?> testImageArray = ImageArrayUtils.readImageArrayRange("test", testFile.getName(), openFile(testFile), testImageBoundaries[0], testImageBoundaries[1]);
            IJ.save(new ImagePlus(null, ImageArrayUtils.toImageProcessor(testImageArray)), "tt.png");
        }
    }

    private int[] getImageBoundaries(ImageProcessor ip) {
//        int minx = ip.getWidth();
//        int miny = ip.getHeight();
//        int maxx = 0;
//        int maxy = 0;
        int min = -1;
        int max = 0;
        for (int pi = 0; pi < ip.getPixelCount(); pi++) {
            int p = ip.get(pi);
            if ((p & 0xFFFFFF) != 0) {
                if (min == -1) min = pi;
                max = pi;
//                int x = pi % ip.getWidth();
//                int y = pi / ip.getWidth();
//                if (x < minx) minx = x;
//                if (y < miny) miny = y;
//                if (x + 1 > maxx) maxx = x + 1;
//                if (y + 1 > maxy) maxy = y + 1;
            }
        }
        return new int[]{
                min, max
        };
    }

    private InputStream openFile(File f) {
        try {
            return new FileInputStream(f);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
