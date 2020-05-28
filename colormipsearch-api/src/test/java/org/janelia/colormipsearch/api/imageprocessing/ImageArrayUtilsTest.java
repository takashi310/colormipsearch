package org.janelia.colormipsearch.api.imageprocessing;

import java.io.FileInputStream;

import ij.ImagePlus;
import org.junit.Test;

public class ImageArrayUtilsTest {

    @Test
    public void readPackBits() throws Exception {
        FileInputStream fs = new FileInputStream("src/test/resources/colormipsearch/api/pacBits.tif");
        ImagePlus testImage = ImageArrayUtils.readPackBits(null, Integer.MAX_VALUE, fs);
        System.out.println();
    }
}
