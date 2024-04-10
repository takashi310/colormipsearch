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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import net.imglib2.img.Img;
import net.imglib2.img.ImgFactory;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.list.ListImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;

import org.janelia.colormipsearch.imageprocessing.ColorTransformation;

public class BidirectionalShapeMatchColorDepthSearchAlgorithm implements ColorDepthSearchAlgorithm<NegativeColorDepthMatchScore>{
    private static final Logger LOG = LoggerFactory.getLogger(GradientBasedNegativeScoreColorDepthSearchAlgorithm.class);
    private static final Set<String> REQUIRED_VARIANT_TYPES = new HashSet<String>() {{
        //add("gradient");
    }};
    private static final int DEFAULT_COLOR_FLUX = 30; // 40um
    private static final int GAP_THRESHOLD = 3;

    private static final TriFunction<Integer, Integer, Integer, Integer> PIXEL_GAP_OP = (gradScorePix, maskPix, dilatedPix) -> {
        if ((maskPix & 0xFFFFFF) != 0 && (dilatedPix & 0xFFFFFF) != 0) {
            int pxGapSlice = GradientAreaGapUtils.calculateSliceGap(maskPix, dilatedPix);
            if (DEFAULT_COLOR_FLUX <= pxGapSlice - DEFAULT_COLOR_FLUX) {
                // negative score value
                return pxGapSlice - DEFAULT_COLOR_FLUX;
            }
        }
        return gradScorePix;
    };

    private final LImage queryImage;
    //private final LImage queryGradientMap;
    //private final LImage queryIntensityValues;
    //private final LImage queryHighExpressionMask; // pix(x,y) = 1 if there's too much expression surrounding x,y
    //private final LImage queryROIMaskImage;
    private final int queryThreshold;
    private final boolean mirrorQuery;
    private final ImageTransformation clearLabels;
    private final ImageProcessing negativeRadiusDilation;
    public final QuadFunction<Integer, Integer, Integer, Integer, Integer> gapOp;

    private final LM_EM_Segmentation segmentator;

    private String tarSegmentedVolumePath;

    BidirectionalShapeMatchColorDepthSearchAlgorithm(LImage queryImage,
                                                     int queryThreshold,
                                                     boolean mirrorQuery,
                                                     String segmentedVolumePath,
                                                     String mask2dPath,
                                                     boolean isEM2LM,
                                                     boolean isBrain,
                                                     ImageTransformation clearLabels,
                                                     ImageProcessing negativeRadiusDilation) {
        this.queryImage = queryImage;
        this.queryThreshold = queryThreshold;
        this.mirrorQuery = mirrorQuery;
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

    public static <T extends Type< T >>  ImageArray<?> convertImgLib2ImgToImageArray(Img<T> img) {
        int width = (int) img.dimension(0);
        int height = (int) img.dimension(1);
        int numPixels = width * height;

        if (img.firstElement() instanceof UnsignedByteType) {
            byte[] array = new byte[numPixels];
            Cursor<T> cursor = img.cursor();
            int i = 0;
            while (cursor.hasNext()) {
                cursor.fwd();
                ByteType p = (ByteType)cursor.get();
                array[i++] = p.get();
            }
            return new ByteImageArray(ImageType.GRAY8, width, height, array);
        } else if (img.firstElement() instanceof UnsignedShortType) {
            short[] array = new short[numPixels];
            Cursor<T> cursor = img.cursor();
            int i = 0;
            while (cursor.hasNext()) {
                cursor.fwd();
                UnsignedShortType p = (UnsignedShortType)cursor.get();
                array[i++] = p.getShort();
            }
            return new ShortImageArray(ImageType.GRAY16, width, height, array);
        } else if (img.firstElement() instanceof ARGBType) {
            int[] array = new int[numPixels];
            Cursor<T> cursor = img.cursor();
            int i = 0;
            while (cursor.hasNext()) {
                cursor.fwd();
                ARGBType p = (ARGBType)cursor.get();
                array[i++] = p.get();
            }
            return new ColorImageArray(ImageType.RGB, width, height, array);
        } else {
            throw new IllegalArgumentException("Unsupported image type");
        }
    }

    public static Img<?> convertImageArrayToImgLib2Img(ImageArray<?> img) {
        int width = img.getWidth();
        int height = img.getHeight();
        int numPixels = width * height;

        if (img instanceof ByteImageArray) {
            byte[] array = ((ByteImageArray) img).getPixels();
            ArrayImgFactory<ByteType> factory = new ArrayImgFactory<>(new ByteType());
            Img<ByteType> imp = factory.create(width, height);
            Cursor<ByteType> cursor = imp.cursor();
            int i = 0;
            while (cursor.hasNext()) {
                cursor.next().set(array[i++]);
            }
            return imp;
        } else if (img instanceof ShortImageArray) {
            short[] array = ((ShortImageArray) img).getPixels();
            ArrayImgFactory<UnsignedShortType> factory = new ArrayImgFactory<>(new UnsignedShortType());
            Img<UnsignedShortType> imp = factory.create(width, height);
            Cursor<UnsignedShortType> cursor = imp.cursor();
            int i = 0;
            while (cursor.hasNext()) {
                cursor.next().set(array[i++]);
            }
            return imp;
        } else if (img instanceof ColorImageArray) {
            byte[] array = ((ColorImageArray) img).getPixels();
            ArrayImgFactory<ARGBType> factory = new ArrayImgFactory<>(new ARGBType());
            Img<ARGBType> imp = factory.create(width, height);
            Cursor<ARGBType> cursor = imp.cursor();
            int i = 0;
            while (cursor.hasNext()) {
                int argb = 0xFF000000 & ((int)array[3*i] << 16) & ((int)array[3*i+1] << 8) & (int)array[3*i+2];
                cursor.next().set(argb);
                i++;
            }
            return imp;
        } else {
            throw new IllegalArgumentException("Unsupported image type");
        }
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
        LImage imp10pxRGBEM = negativeRadiusDilation.applyTo(queryImage.map(ColorTransformation.mask(queryThreshold)));
        LImage gaps = LImageUtils.combine4(
                segmentedCDMMask1,
                emMaskGradient,
                segmentedCDM,
                imp10pxRGBEM,
                gapOp.andThen(gap -> gap > GAP_THRESHOLD ? gap : 0)
        );
        long EMtoSampleNegativeScore = gaps.fold(0L, Long::sum);


        LImage imp10pxRGBLM = negativeRadiusDilation.applyTo(segmentedCDM.map(ColorTransformation.mask(queryThreshold)));
        Img<UnsignedShortType> originalGradientImg = (Img<UnsignedShortType>)DistanceTransform.GenerateDistanceTransform(segmentedCDMImg, 10);
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
