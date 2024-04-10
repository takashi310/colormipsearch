package org.janelia.colormipsearch.api_v2.cdsearch;

import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.Collections;

import ij.ImagePlus;
import ij.io.Opener;
import io.scif.SCIFIO;
import io.scif.config.SCIFIOConfig;
import io.scif.img.IO;
import io.scif.img.ImgSaver;
import net.imglib2.Cursor;
import net.imglib2.algorithm.morphology.Dilation;
import net.imglib2.algorithm.neighborhood.CenteredRectangleShape;
import net.imglib2.algorithm.stats.ComputeMinMax;
import net.imglib2.algorithm.stats.Max;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import org.checkerframework.common.value.qual.EnsuresMinLenIf;
import org.janelia.colormipsearch.api_v2.bdssearch.*;
import org.janelia.colormipsearch.imageprocessing.*;
import org.junit.Test;

import javax.imageio.ImageIO;

import static org.janelia.colormipsearch.api_v2.bdssearch.ImageZProjection.maxIntensityProjection;
import static org.janelia.colormipsearch.api_v2.cdsearch.BidirectionalShapeMatchColorDepthSearchAlgorithm.convertImageArrayToImgLib2Img;
import static org.janelia.colormipsearch.api_v2.cdsearch.BidirectionalShapeMatchColorDepthSearchAlgorithm.convertImgLib2ImgToImageArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

public class BidirectionalShapeMatchingTest {

    public static void saveAsTiff(Img<?> img, String filePath) {
        SCIFIO scifio = new SCIFIO();
        ImgSaver saver = new ImgSaver(scifio.context());

        // Check if the output file exists. If yes, delete it to allow overwriting.
        File outputFile = new File(filePath);
        if (outputFile.exists()) {
            boolean deleted = outputFile.delete();
            if (!deleted) {
                System.err.println("Failed to delete existing file: " + filePath);
                return;
            }
        }

        try {
            // Use SCIFIO to save the image as TIFF
            saver.saveImg(filePath, img, new SCIFIOConfig().writerSetCompression("LZW"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            saver.context().dispose();
            scifio.context().dispose();
        }
    }

    public static BufferedImage convertToBufferedImage(Img<ARGBType> img) {
        // Assuming the image is non-empty, get its dimensions
        int width = (int) img.max(0) + 1;
        int height = (int) img.max(1) + 1;

        // Create a BufferedImage of type TYPE_INT_ARGB
        BufferedImage bufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);

        // Use a cursor to iterate over the Img<ARGBType>
        Cursor<ARGBType> cursor = img.cursor();

        while (cursor.hasNext()) {
            cursor.fwd();
            int x = cursor.getIntPosition(0);
            int y = cursor.getIntPosition(1);
            int argb = cursor.get().get();

            // Set the ARGB value in the BufferedImage
            bufferedImage.setRGB(x, y, argb);
        }

        return bufferedImage;
    }

    public static void saveImageAsPNG(Img<ARGBType> image, String outputPath) {
        // Convert Img<ARGBType> to BufferedImage
        BufferedImage buffered_image = convertToBufferedImage(image);
        // Use ImageIO to write the BufferedImage to a file
        try {
            File outputFile = new File(outputPath);
            ImageIO.write(buffered_image, "PNG", outputFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void pixelMatchScore() {
        final String em_path = "src/test/resources/colormipsearch/api/cdsearch/1537331894.swc";
        final String target_cdm = "src/test/resources/colormipsearch/api/cdsearch/GMR_31G04_AE_01-20190813_66_F3-40x-Brain-JRC2018_Unisex_20x_HR-2704505419467849826-CH2-07_CDM.tif";
        final String mask2d_path = "src/test/resources/colormipsearch/api/cdsearch/MAX_JRC2018_UNISEX_20x_HR_2DMASK.tif";
        final String tarSegmentedVolumePath = "src/test/resources/colormipsearch/api/cdsearch/test_TargetSegmentedLM.tif";

        int queryThreshold = 20;
        int negativeRadius = 10;

        Img<IntegerType> segmentedVolume = (Img<IntegerType>) SWCDraw.draw(em_path, 1210, 566, 174, 0.5189161, 0.5189161, 1.0, 2, true);
        Img<ARGBType> queryImageImg = LM_EM_Segmentation.GenerateCDM(segmentedVolume, mask2d_path);
        ColorImageArray queryImageArray = (ColorImageArray)convertImgLib2ImgToImageArray(queryImageImg);
        LImage queryImage = LImageUtils.create(queryImageArray);

        Img<IntegerType> tarSegmentedVolume = ( Img<IntegerType> ) IO.openImgs(tarSegmentedVolumePath).get(0);
        ContrastEnhancer.scaleHistogram(tarSegmentedVolume, 3, 0, 255, 0);
        Img<ARGBType> targetImageImg = LM_EM_Segmentation.GenerateCDM(tarSegmentedVolume, mask2d_path);
        ColorImageArray targetImageArray = (ColorImageArray)convertImgLib2ImgToImageArray(targetImageImg);

        long start, end;
        start = System.currentTimeMillis();

        ImageRegionGenerator ignoredRegionsProvider = (img -> (x, y) -> x >= img.getWidth() - 260 && y < 90 || x < 330 && y < 100);

        ImageTransformation clearIgnoredRegions = ImageTransformation.clearRegion(ignoredRegionsProvider.getRegion(queryImageArray));

        ImageProcessing negativeRadiusDilation = ImageProcessing.create(clearIgnoredRegions)
                .applyColorTransformation(ColorTransformation.mask(queryThreshold))
                .unsafeMaxFilter(negativeRadius);

        BidirectionalShapeMatchColorDepthSearchAlgorithm maskNegativeScoresCalculator = new BidirectionalShapeMatchColorDepthSearchAlgorithm(
                queryImage,
                20,
                false,
                em_path,
                mask2d_path,
                true,
                true,
                clearIgnoredRegions,
                negativeRadiusDilation
        );

        Img<IntegerType> querySegmentedVolumeResult = maskNegativeScoresCalculator.getSegmentedQueryVolumeImage();

        maskNegativeScoresCalculator.setTargetSegmentedVolumePath(tarSegmentedVolumePath);

        NegativeColorDepthMatchScore score = maskNegativeScoresCalculator.calculateMatchingScore(targetImageArray, Collections.emptyMap());

        System.out.println("score: " + score.getScore());

        end = System.currentTimeMillis();
        System.out.println("time: "+((float)(end-start)/1000)+"sec");
    }


}