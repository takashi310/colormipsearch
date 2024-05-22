package org.janelia.colormipsearch.api_v2.cdsearch;

import ij.process.ImageProcessor;
import net.imglib2.algorithm.morphology.Dilation;
import net.imglib2.algorithm.neighborhood.CenteredRectangleShape;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import org.janelia.colormipsearch.api_v2.bdssearch.DistanceTransform;
import org.janelia.colormipsearch.api_v2.bdssearch.LM_EM_Segmentation;
import org.janelia.colormipsearch.imageprocessing.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import net.imglib2.img.Img;

import org.janelia.colormipsearch.imageprocessing.ColorTransformation;

import static org.janelia.colormipsearch.api_v2.bdssearch.ImgUtils.*;
import static org.janelia.colormipsearch.api_v2.bdssearch.ImgUtils.convertImageArrayToImgLib2Img;
import static org.janelia.colormipsearch.api_v2.bdssearch.MaximumFilter.apply2D_ARGB;

public class BidirectionalShapeMatchColorDepthSearchAlgorithm implements ColorDepthSearchAlgorithm<NegativeColorDepthMatchScore>{
    private static final Set<String> REQUIRED_VARIANT_TYPES = new HashSet<String>() {{
        //add("gradient");
    }};
    private static final int DEFAULT_COLOR_FLUX = 30; // 40um
    private static final int GAP_THRESHOLD = 3;

    private static final TriFunction<Integer, Integer, Integer, Integer> PIXEL_GAP_OP = (gradScorePix, maskPix, dilatedPix) -> {
        // The slice gap was not used in the original plugin.
        /*
        if ((maskPix & 0xFFFFFF) != 0 && (dilatedPix & 0xFFFFFF) != 0) {
            int pxGapSlice = GradientAreaGapUtils.calculateSliceGap(maskPix, dilatedPix);
            if (DEFAULT_COLOR_FLUX <= pxGapSlice - DEFAULT_COLOR_FLUX) {
                // negative score value
                return pxGapSlice - DEFAULT_COLOR_FLUX;
            }
        }
        */
        return gradScorePix;
    };

    private final LImage queryImage;
    private final int queryThreshold;
    private final ImageTransformation clearLabels;
    private final ImageProcessing negativeRadiusDilation;
    public final QuadFunction<Integer, Integer, Integer, Integer, Integer> gapOp;

    private final LM_EM_Segmentation segmentator;

    private String tarSegmentedVolumePath;

    BidirectionalShapeMatchColorDepthSearchAlgorithm(LImage queryImage,
                                                     int queryThreshold,
                                                     String segmentedVolumePath,
                                                     String mask2dPath,
                                                     boolean isEM2LM,
                                                     boolean isBrain,
                                                     ImageTransformation clearLabels,
                                                     ImageProcessing negativeRadiusDilation) {
        this.queryImage = queryImage;
        this.queryThreshold = queryThreshold;
        this.clearLabels = clearLabels;
        this.negativeRadiusDilation = negativeRadiusDilation;
        gapOp = (p1, p2, p3, p4) -> PIXEL_GAP_OP.apply(p1 * p2, p3, p4);
        segmentator = new LM_EM_Segmentation(segmentedVolumePath, mask2dPath, isEM2LM, isBrain);
    }

    @Override
    public ImageArray<?> getQueryImage() {
        return queryImage.toImageArray();
    }

    @Override
    public int getQuerySize() {
        return queryImage.fold(0, (pix, s) -> {
            int red = (pix >> 16) & 0xff;
            int green = (pix >> 8) & 0xff;
            int blue = pix & 0xff;

            if (red > queryThreshold || green > queryThreshold || blue > queryThreshold) {
                return s + 1;
            } else {
                return s;
            }
        });
    }

    @Override
    public int getQueryFirstPixelIndex() {
        return findQueryFirstPixelIndex();
    }

    private int findQueryFirstPixelIndex() {
        return queryImage.foldi(-1, (x, y, pix, res) -> {
            if (res == -1) {
                int red = (pix >> 16) & 0xff;
                int green = (pix >> 8) & 0xff;
                int blue = pix & 0xff;

                if (red > queryThreshold || green > queryThreshold || blue > queryThreshold) {
                    return y * queryImage.width() + x;
                } else {
                    return res;
                }
            } else {
                return res;
            }
        });
    }

    @Override
    public int getQueryLastPixelIndex() {
        return findQueryLastPixelIndex();
    }

    private int findQueryLastPixelIndex() {
        return queryImage.foldi(-1, (x, y, pix, res) -> {
            int red = (pix >> 16) & 0xff;
            int green = (pix >> 8) & 0xff;
            int blue = pix & 0xff;

            if (red > queryThreshold || green > queryThreshold || blue > queryThreshold) {
                int index = y * queryImage.width() + x;
                if (index > res) {
                    return index;
                } else {
                    return res;
                }
            } else {
                return res;
            }
        });
    }

    public void setTargetSegmentedVolumePath(String path) {
        tarSegmentedVolumePath = path;
    }

    public Img<IntegerType> getSegmentedQueryVolumeImage() {
        return segmentator.getSegmentedQueryImage();
    }

    @Override
    public Set<String> getRequiredTargetVariantTypes() {
        return REQUIRED_VARIANT_TYPES;
    }

    /**
     * @param targetImageArray
     * @param variantTypeSuppliers
     * @return
     */
    @Override
    public NegativeColorDepthMatchScore calculateMatchingScore(@Nonnull ImageArray<?> targetImageArray,
                                                               Map<String, Supplier<ImageArray<?>>> variantTypeSuppliers) {

        Img<ARGBType> segmentedCDMImg = segmentator.Run(tarSegmentedVolumePath);
        ColorImageArray segmentedCDMImageArray = (ColorImageArray)convertImgLib2ImgToImageArray(segmentedCDMImg);
        LImage segmentedCDM = LImageUtils.create(segmentedCDMImageArray);
        LImage segmentedCDMMask1 = segmentedCDM.map(ColorTransformation.rgbToSignal(1));

        Img<ARGBType> emMask = (Img<ARGBType>)convertImageArrayToImgLib2Img(queryImage.toImageArray());
        Img<UnsignedShortType> emMaskGradientImg = (Img<UnsignedShortType>)DistanceTransform.GenerateDistanceTransform(emMask, 5);
        ShortImageArray emMaskGradientImageArray = (ShortImageArray)convertImgLib2ImgToImageArray(emMaskGradientImg);
        LImage emMaskGradient = LImageUtils.create(emMaskGradientImageArray);
        ColorImageArray imp10pxRGBEMImageArray = (ColorImageArray)convertImgLib2ImgToImageArray(apply2D_ARGB(emMask, 10, queryThreshold));
        LImage imp10pxRGBEM = LImageUtils.create(imp10pxRGBEMImageArray);

        LImage gaps = LImageUtils.combine4(
                segmentedCDMMask1,
                emMaskGradient,
                segmentedCDM,
                imp10pxRGBEM,
                gapOp.andThen(gap -> gap > GAP_THRESHOLD ? gap : 0)
        );

        long EMtoSampleNegativeScore = gaps.fold(0L, Long::sum);

        Img<ARGBType> dilatedsegmentedCDMImg = apply2D_ARGB(segmentedCDMImg, 10, queryThreshold);
        ColorImageArray imp10pxRGBLMImageArray = (ColorImageArray)convertImgLib2ImgToImageArray(dilatedsegmentedCDMImg);
        LImage imp10pxRGBLM = LImageUtils.create(imp10pxRGBLMImageArray);
        Img<UnsignedShortType> originalGradientImg = (Img<UnsignedShortType>)DistanceTransform.GenerateDistanceTransformWithoutDilation(dilatedsegmentedCDMImg);
        ShortImageArray originalGradientImageArray = (ShortImageArray)convertImgLib2ImgToImageArray(originalGradientImg);
        LImage originalGradient = LImageUtils.create(originalGradientImageArray);
        LImage queryMask1 = queryImage.map(ColorTransformation.rgbToSignal(queryThreshold));

        LImage gaps2 = LImageUtils.combine4(
                queryMask1,
                originalGradient,
                queryImage,
                imp10pxRGBLM,
                gapOp.andThen(gap -> gap > GAP_THRESHOLD ? gap : 0)
        );
        long SampleToMask = gaps2.fold(0L, Long::sum);

        long score = (SampleToMask + EMtoSampleNegativeScore) / 2;

        return new NegativeColorDepthMatchScore(score, 0, false);
    }

    private ImageArray<?> getVariantImageArray(Map<String, Supplier<ImageArray<?>>> variantTypeSuppliers, String variantType) {
        Supplier<ImageArray<?>> variantImageArraySupplier = variantTypeSuppliers.get(variantType);
        if (variantImageArraySupplier != null) {
            return variantImageArraySupplier.get();
        } else {
            return null;
        }
    }

}
