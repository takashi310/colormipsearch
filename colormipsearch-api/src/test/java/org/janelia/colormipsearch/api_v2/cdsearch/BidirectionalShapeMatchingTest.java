package org.janelia.colormipsearch.api_v2.cdsearch;

import java.util.Collections;

import net.imglib2.img.Img;
import net.imglib2.type.numeric.ARGBType;
import org.janelia.colormipsearch.imageprocessing.*;
import org.janelia.colormipsearch.model.FileData;
import org.junit.Test;

import static org.janelia.colormipsearch.api_v2.bdssearch.ImgUtils.convertImgLib2ImgToImageArray;
import static org.janelia.colormipsearch.api_v2.bdssearch.ImgUtils.loadImageFromFileData;
import static org.junit.Assert.assertEquals;

public class BidirectionalShapeMatchingTest {
    @Test
    public void bidirectionalShapeMatchingScore() {
        final String em_path = "src/test/resources/colormipsearch/api/cdsearch/27329.swc";
        final String em_cdm_path = "src/test/resources/colormipsearch/api/cdsearch/27329.png";
        final String mask2d_path = "src/test/resources/colormipsearch/api/cdsearch/MAX_JRC2018_UNISEX_20x_HR_2DMASK.tif";
        final String tarSegmentedVolumePath = "src/test/resources/colormipsearch/api/cdsearch/1_VT000770_130A10_AE_01-20180810_61_G2-m-CH1_02__gen1_MCFO.nrrd";
        final String targetCDMImagePath = "src/test/resources/colormipsearch/api/cdsearch/0342_VT000770_130A10_AE_01-20180810_61_G2-m-CH1_02.png";

        int queryThreshold = 20;
        int negativeRadius = 10;

        Img<ARGBType> queryImageImg = (Img<ARGBType>) loadImageFromFileData(FileData.fromString(em_cdm_path));
        ImageArray<?> queryImageArray = convertImgLib2ImgToImageArray(queryImageImg);
        ImageRegionGenerator ignoredRegionsProvider = (img -> (x, y) -> x >= img.getWidth() - 260 && y < 90 || x < 330 && y < 100);
        ImageTransformation clearIgnoredRegions = ImageTransformation.clearRegion(ignoredRegionsProvider.getRegion(queryImageArray));
        LImage queryImage = LImageUtils.create(queryImageArray).mapi(clearIgnoredRegions);

        Img<ARGBType> targetImageImg = (Img<ARGBType>) loadImageFromFileData(FileData.fromString(targetCDMImagePath));
        ImageArray<?> targetImageArray = convertImgLib2ImgToImageArray(targetImageImg);

        long start, end;
        start = System.currentTimeMillis();

        ImageProcessing negativeRadiusDilation = ImageProcessing.create(clearIgnoredRegions)
                .applyColorTransformation(ColorTransformation.mask(queryThreshold))
                .unsafeMaxFilter(negativeRadius);

        BidirectionalShapeMatchColorDepthSearchAlgorithm maskNegativeScoresCalculator = new BidirectionalShapeMatchColorDepthSearchAlgorithm(
                queryImage,
                20,
                em_path,
                mask2d_path,
                true,
                true,
                clearIgnoredRegions,
                negativeRadiusDilation
        );

        end = System.currentTimeMillis();
        System.out.println("Query Segmentation elapsed time: "+((float)(end-start)/1000)+"sec");

        start = System.currentTimeMillis();

        maskNegativeScoresCalculator.setTargetSegmentedVolumePath(tarSegmentedVolumePath);
        NegativeColorDepthMatchScore score = maskNegativeScoresCalculator.calculateMatchingScore(targetImageArray, Collections.emptyMap());

        System.out.println("score: " + score.getScore());

        end = System.currentTimeMillis();
        System.out.println("Score Calculation elapsed time: "+((float)(end-start)/1000)+"sec");

        assertEquals(506, score.getScore());
    }


}