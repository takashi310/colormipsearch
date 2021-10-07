package org.janelia.colormipsearch.api.cdsearch;

import java.util.Collections;

import ij.ImagePlus;
import ij.io.Opener;
import org.janelia.colormipsearch.api.imageprocessing.ImageArray;
import org.janelia.colormipsearch.api.imageprocessing.ImageArrayUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PixelMatchColorDepthSearchAlgorithmTest {

    @Test
    public void pixelMatchScore() {
        ImagePlus testMask = new Opener().openTiff("src/test/resources/colormipsearch/api/cdsearch/1752016801-LPLC2-RT_18U.tif", 1);
        ImagePlus testTarget = new Opener().openTiff("src/test/resources/colormipsearch/api/cdsearch/GMR_31G04_AE_01-20190813_66_F3-40x-Brain-JRC2018_Unisex_20x_HR-2704505419467849826-CH2-07_CDM.tif", 1);
        ImageArray<?> testMaskArray = ImageArrayUtils.fromImagePlus(testMask);
        ImageArray<?> testTargetArray = ImageArrayUtils.fromImagePlus(testTarget);
        PixelMatchColorDepthSearchAlgorithm colorDepthSearchAlgorithm = new PixelMatchColorDepthSearchAlgorithm(
            testMaskArray,
            20,
            true,
            null,
            0,
            false,
            20,
            0.01,
            2,
            img -> (x, y) -> x >= img.getWidth() - 260 && y < 90 || x < 330 && y < 100
        );
        ColorMIPMatchScore score = colorDepthSearchAlgorithm.calculateMatchingScore(testTargetArray, Collections.emptyMap());
        assertTrue(score.isMatch());
        assertEquals(87, score.getMatchingPixNum());
        assertFalse(score.isMirrored());
    }


}
