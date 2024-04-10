package org.janelia.colormipsearch.api_v2.bdssearch;

import java.io.File;
import java.lang.String;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import io.scif.SCIFIO;
import io.scif.config.SCIFIOConfig;
import io.scif.img.ImgSaver;
import net.imglib2.Cursor;
import net.imglib2.RandomAccess;
import net.imglib2.algorithm.morphology.Dilation;
import net.imglib2.algorithm.neighborhood.CenteredRectangleShape;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.numeric.ARGBType;
import io.scif.img.IO;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.view.IntervalView;
import net.imglib2.view.Views;
import net.imglib2.algorithm.stats.ComputeMinMax;
import net.imglib2.algorithm.stats.Max;
import org.janelia.colormipsearch.imageprocessing.ColorImageArray;

import static org.janelia.colormipsearch.api_v2.bdssearch.ImageFlipper.flipHorizontally;
import static org.janelia.colormipsearch.api_v2.bdssearch.ImageZProjection.maxIntensityProjection;

public class LM_EM_Segmentation {
    String segVolume = "LM";//LM is from EM to LM searach, EM is from LM to EM search
    int XYmaxSize = 7; //(or 10) 5 is 5 microns for the segmentation mask dilation
    int ZmaxSize = 4; //3 or 5 (x2 microns)
    String tissue = "brain";
    String mask2DPath = "";
    Img< IntegerType > segResult = null;
    Img< IntegerType > segmentedVolumeBeforeFlipping = null;

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

    public static void fillWithBlackUsingCursor(Img<ARGBType> image, long offsetX, long offsetY, long width, long height) {
        // Define the region as a rectangle [minX, minY] to [maxX, maxY]
        long[] min = new long[]{offsetX, offsetY}; // Starting coordinates of the rectangle
        long[] max = new long[]{offsetX+width, offsetY+height}; // Ending coordinates of the rectangle

        // Create a view on the specified interval
        IntervalView<ARGBType> region = Views.interval(image, min, max);

        Cursor<ARGBType> cursor = region.cursor();
        while (cursor.hasNext()) {
            cursor.next().set(0xFF000000);
        }
    }

    public static <T extends RealType<T>> void fillWithZeroUsingCursor(Img<T> image, long offsetX, long offsetY, long width, long height) {
        // Define the region as a rectangle [minX, minY] to [maxX, maxY]
        long[] min = new long[]{offsetX, offsetY}; // Starting coordinates of the rectangle
        long[] max = new long[]{offsetX+width, offsetY+height}; // Ending coordinates of the rectangle

        // Create a view on the specified interval
        IntervalView<T> region = Views.interval(image, min, max);

        Cursor<T> cursor = region.cursor();
        while (cursor.hasNext()) {
            cursor.next().setReal(0.0);
        }
    }

    public Img<IntegerType> getSegmentedQueryImage() {
        return segResult;
    }

    public LM_EM_Segmentation(String segmented_volume_path, String mask2d_path, boolean isEM2LM, boolean isBrain) {
        if (isEM2LM)
            segVolume = "LM";
        else
            segVolume = "EM";

        if (isBrain)
            tissue = "brain";
        else
            tissue = "others";

        mask2DPath = mask2d_path;

        Initialize(segmented_volume_path);
    }

    public void Initialize(String segmentedVolumePath) {
        String fileext="swc";
        int i = segmentedVolumePath.lastIndexOf('.');
        if (i > 0) {
            fileext = segmentedVolumePath.substring(i+1);
        }

        if(segVolume.equals("EM")){ // LM to EM search
            //maskLM_STOri = open(segmentedLMdir+maskLM_ST+".nrrd");
            segmentedVolumeBeforeFlipping = ( Img<IntegerType> ) IO.openImgs(segmentedVolumePath).get(0);
            segResult = flipHorizontally(segmentedVolumeBeforeFlipping);
            fileext="nrrd";
        }
        else if(segVolume.equals("LM")) { // EM to LM search
            Img<IntegerType> segmentedVolume = null;
            if (fileext.equals("swc")) {
                if (tissue == "brain")
                    //run("swc draw single 3d", "input="+segmentedLMdir+maskLM_ST+".swc width=605 height=283 depth=87 voxel_w=1.0378322 voxel_h=1.0378322 voxel_d=2.0000000 radius=1 ignore");
                    segmentedVolume = (Img<IntegerType>) SWCDraw.draw(segmentedVolumePath, 605, 283, 87, 1.0378322, 1.0378322, 2.0, 1, true);
                else
                    //run("swc draw single 3d", "input="+segmentedLMdir+maskLM_ST+".swc width=573 height=1119 depth=219 voxel_w=0.4611220 voxel_h=0.4611220 voxel_d=0.7 radius=1 ignore");
                    segmentedVolume = (Img<IntegerType>) SWCDraw.draw(segmentedVolumePath, 573, 1119, 219, 0.4611220, 0.4611220, 0.7, 1, true);
            } else {
                segmentedVolume = (Img<IntegerType>) IO.openImgs(segmentedVolumePath).get(0);
                //open(segmentedLMdir + maskLM_ST + ".nrrd");
            }

            //bitd=bitDepth();
            Img<IntegerType> zProjectedSegVolume = maxIntensityProjection(segmentedVolume, 0);
            ContrastEnhancer.stretchHistogram(zProjectedSegVolume, 0.35, -1, -1); // Stretch histogram with 0.35% saturated pixels
            IntegerType min = (IntegerType) zProjectedSegVolume.firstElement().createVariable();
            IntegerType max = (IntegerType) zProjectedSegVolume.firstElement().createVariable();
            ComputeMinMax.computeMinMax(zProjectedSegVolume, min, max);

            //segmentedVolume selectWindow(maskLM_ST+"."+fileext);

            if (max.getRealDouble() != 255) {
                //setMinAndMax(0, max);
                //run("Apply LUT", "stack");
                double newMin = 0.0;
                double newMax = 255.0;
                double scale = (newMax - newMin) / (max.getRealDouble() - min.getRealDouble());
                double offset = newMin - min.getRealDouble() * scale;

                // Apply scaling to each pixel
                Cursor<IntegerType> cursor = segmentedVolume.cursor();
                while (cursor.hasNext()) {
                    IntegerType pixel = cursor.next();
                    double scaledValue = pixel.getRealDouble() * scale + offset;
                    pixel.setReal(scaledValue);
                }
            }

            Img<IntegerType> scaled_segmentedVolume = null;
            if (tissue.equals("brain"))
                scaled_segmentedVolume = Scale3DImage.scaleImage(segmentedVolume, 605, 283, 87); //run("Size...", "width=605 height=283 depth=87 constrain average interpolation=Bicubic");
            else
                scaled_segmentedVolume = Scale3DImage.scaleImage(segmentedVolume, 287, 560, 110);//run("Size...", "width=287 height=560 depth=110 average interpolation=Bicubic");

            ArrayList<CenteredRectangleShape> shapes = new ArrayList<CenteredRectangleShape>();
            int[] span_max_x = {XYmaxSize, 0, 0};
            CenteredRectangleShape neighborhoodX = new CenteredRectangleShape(span_max_x, true); // true for including center
            shapes.add(neighborhoodX);
            int[] span_max_y = {0, XYmaxSize, 0};
            CenteredRectangleShape neighborhoodY = new CenteredRectangleShape(span_max_y, true); // true for including center
            shapes.add(neighborhoodY);
            int[] span_max_z = {0, 0, ZmaxSize};
            CenteredRectangleShape neighborhoodZ = new CenteredRectangleShape(span_max_z, true); // true for including center
            shapes.add(neighborhoodZ);
            // Perform the dilation (maximum filter)
            Img<IntegerType> dilated_segmentedVolume = Dilation.dilate(scaled_segmentedVolume, shapes, 1);

            Img<IntegerType> scaled_segmentedVolume2 = null;
            if (tissue.equals("brain"))
                scaled_segmentedVolume2 = Scale3DImage.scaleImage(dilated_segmentedVolume, 1210, 566, 174); //run("Size...", "width=1210 height=566 depth=174 constrain average interpolation=Bicubic");
            else
                scaled_segmentedVolume2 = Scale3DImage.scaleImage(dilated_segmentedVolume, 572, 1119, 219); //run("Size...", "width=573 height=1119 depth=219 average interpolation=Bicubic");

            //run("Max value");
            Cursor<IntegerType> maxCur = Max.findMax(scaled_segmentedVolume2);
            int maxvalue = maxCur.get().getInteger();

            if (maxvalue > 2000)
                segResult = ImageThresholding.createBinaryImage(scaled_segmentedVolume2, 2000, 65535);
            else
                segResult = ImageThresholding.createBinaryImage(scaled_segmentedVolume2, 20, 65535);
        }
    }

    public Img<ARGBType> Run(String tarSegmentedVolumePath) {
        Img<ARGBType> andCDM = null;
        if(segResult != null){
            Img<IntegerType> tarSegmentedVolume = null;
            Img<IntegerType> tarSegmentedVolumeBeforeFlipping = null;
            Img<IntegerType> tarSegResult = null;
            if(segVolume.equals("EM")){
                if(tissue.equals("brain"))
                    tarSegmentedVolume = (Img<IntegerType>) SWCDraw.draw(tarSegmentedVolumePath, 1210, 566, 174, 0.5189161, 0.5189161, 1.0, 20, true);
                else
                    tarSegmentedVolume = (Img<IntegerType>) SWCDraw.draw(tarSegmentedVolumePath, 573, 1119, 219, 0.4611220, 0.4611220, 0.7, 20, true);
                tarSegResult = ImageThresholding.createBinaryImage( tarSegmentedVolume,4, 200);
            }
            if(segVolume.equals("LM")){
                tarSegmentedVolumeBeforeFlipping = ( Img<IntegerType> ) IO.openImgs(tarSegmentedVolumePath).get(0);
                tarSegResult = flipHorizontally(tarSegmentedVolumeBeforeFlipping);
            }//if(segVolume=="LM"){

            saveAsTiff(tarSegResult, "/Users/kawaset/cdm_test/tarSegResult.tif");

            Img<IntegerType> tarMaskedSegmentedVolume = ImageANDOperation.andOperation(tarSegResult, segResult);
            IntegerType min = (IntegerType) tarMaskedSegmentedVolume.firstElement().createVariable();
            IntegerType max = (IntegerType) tarMaskedSegmentedVolume.firstElement().createVariable();
            ComputeMinMax.computeMinMax(tarMaskedSegmentedVolume, min, max);

            saveAsTiff(tarMaskedSegmentedVolume, "/Users/kawaset/cdm_test/tarMaskedSegmentedVolume.tif");

            int maxvalue = max.getInteger();
            long volumeRight = 0;

            if(maxvalue > 25){
                tarMaskedSegmentedVolume = ThreeDconnect_component(tarMaskedSegmentedVolume);
                volumeRight = VoxelCounter.countNonZeroVoxels(tarMaskedSegmentedVolume);
            }

            saveAsTiff(tarMaskedSegmentedVolume, "/Users/kawaset/cdm_test/ThreeDconnect_component.tif");

            Img<IntegerType> tarSegResultFL = tarSegmentedVolumeBeforeFlipping != null ? tarSegmentedVolumeBeforeFlipping : segmentedVolumeBeforeFlipping ;
            Img<IntegerType> tarMaskedSegmentedVolumeFL = ImageANDOperation.andOperation(tarSegResultFL, segResult);
            IntegerType minFL = (IntegerType) tarMaskedSegmentedVolumeFL.firstElement().createVariable();
            IntegerType maxFL = (IntegerType) tarMaskedSegmentedVolumeFL.firstElement().createVariable();
            ComputeMinMax.computeMinMax(tarMaskedSegmentedVolumeFL, minFL, maxFL);

            saveAsTiff(tarSegResultFL, "/Users/kawaset/cdm_test/tarSegResultFL.tif");
            saveAsTiff(tarMaskedSegmentedVolumeFL, "/Users/kawaset/cdm_test/tarMaskedSegmentedVolumeFL.tif");

            int maxvalueFL = maxFL.getInteger();
            long volumeFL = 0;

            if(maxvalueFL > 25 ){
                tarMaskedSegmentedVolumeFL = ThreeDconnect_component(tarMaskedSegmentedVolumeFL);
                volumeFL = VoxelCounter.countNonZeroVoxels(tarMaskedSegmentedVolumeFL);
            }

            saveAsTiff(tarMaskedSegmentedVolumeFL, "/Users/kawaset/cdm_test/ThreeDconnect_component2.tif");

            if(volumeRight >= volumeFL){
                if(tarMaskedSegmentedVolume != null){
                    andCDM = GenerateCDM(tarMaskedSegmentedVolume, mask2DPath);
                    //run("Select All");
                    //run("Copy");
                }
                //CDMname=hitEM
            }else{
                if(tarMaskedSegmentedVolumeFL != null) {
                    andCDM = GenerateCDM(tarMaskedSegmentedVolumeFL, mask2DPath);
                    //CDMname=hitEM+"_FL";
                }
            }
        }//if(SkipSeg==0){

        return andCDM;
    }

    public static <T extends IntegerType< T >> Img<ARGBType> GenerateCDM(Img<T> input, String mask2DPath){
        boolean easyADJ=true;
        String AutoBRVST="Segmentation based no lower value cut";
        int AutoBRV=0;
        boolean GradientDim=false;
        boolean CLAHE=true;
        boolean colorcoding=true;
        int neuronimg=0;
        int autothre=0;//1 is FIJI'S threshold, 0 is DSLT thresholding
        boolean colorscale=false;
        boolean reverse0=false;
        int desiredmean=198;
        String usingLUT="PsychedelicRainBow2";
        int startMIP=0;
        int endMIP=1000;
        boolean expand=false;
        int gammavalue=1;

        Img<T> zProjectedImage = ImageZProjection.maxIntensityProjection(input, 0);
        T minT = zProjectedImage.firstElement().createVariable();
        T maxT = zProjectedImage.firstElement().createVariable();
        ComputeMinMax.computeMinMax(zProjectedImage, minT, maxT);
        int max = maxT.getInteger();
        int Inimin = minT.getInteger();

        int DefMaxValue = 65535;
        if(max>255 && max<4096)
            DefMaxValue=4095;
        else if (max>4095)
            DefMaxValue=65535;
        else if (max<256)
            DefMaxValue=255;
        ContrastEnhancer.stretchHistogram(zProjectedImage, 0.3, -1, -1);
        ComputeMinMax.computeMinMax(zProjectedImage, minT, maxT);
        int Inimax = maxT.getInteger();
        int min = minT.getInteger();
        int RealInimax=Inimax;
        if(DefMaxValue==4095){
            if(Inimax<200 && Inimax>100)
                Inimax=(int)Math.round(Inimax*1.5);
            else if (Inimax>=200 && Inimax<300)
                Inimax=(int)Math.round(Inimax*1.2);
            else if (Inimax<100)
                Inimax=(int)Math.round(Inimax*2);
            else if(Inimax<2000 && Inimax>1000)
                Inimax=(int)Math.round(Inimax*0.9);
            else if(Inimax>=2000)
                Inimax=(int)Math.round(Inimax*0.8);
        }
        if(DefMaxValue==65535){
            if(Inimax<3200 && Inimax>1600)
                Inimax=(int)Math.round(Inimax*1.5);
            else if (Inimax>=3200 && Inimax<4800)
                Inimax=(int)Math.round(Inimax*1.2);
            else if (Inimax<1600)
                Inimax=(int)Math.round(Inimax*2);
            else if (Inimax>=4800 && Inimax<8000)
                Inimax=(int)Math.round(Inimax*1.1);
        }

        int applyV = Math.round(Inimax);
        ContrastEnhancer.scaleHistogram(zProjectedImage, Inimax, 0, DefMaxValue, 0);

        if(easyADJ){
            long sumval=0;
            long sumnumpx=0;
            Cursor<T> cursor = zProjectedImage.cursor();
            while (cursor.hasNext()) {
                cursor.fwd();
                int val = cursor.get().getInteger();
                if(val>1){
                    sumval=sumval+val;
                    sumnumpx=sumnumpx+1;
                }
            }

            long aveval = Math.round((double)sumval/sumnumpx/16);

            if(DefMaxValue!=65535){
                if(Inimax > aveval && aveval > 0)
                    applyV = (int)aveval;
            }
        }//if(easyADJ==true){

        int width = (int)input.dimension(0);
        int height = (int)input.dimension(1);
        int slices = (int)input.dimension(2);

        if(width>height)
            expand=false;
        else
            expand=true;

        long zerovalue = 0;
        int zeroexi = 0;
        String MaskName2D = "";
        String Mask3D = "";

        if(width==1401 && height==2740 && slices==402){//1401x2740x402, JRC2018 63x UNISEX
            MaskName2D="MAX_JRC2018_VNC_UNISEX_63x_2DMASK"; Mask3D="JRC2018_VNC_UNISEX_63x_3DMASK.nrrd";
        }else if(width==1402 && height==2851 && slices==377){//1402x2851x377), JRC2018 63x Female
            MaskName2D="MAX_JRC2018_VNC_FEMALE_63x_2DMASK.tif"; Mask3D="JRC2018_VNC_FEMALE_63x_3DMASK.nrrd";
        }else if(width==1401 && height==2851 && slices==422){//1401x2851x422), JRC2018 63x MALE
            MaskName2D="MAX_JRC2018_VNC_MALE_63x_2DMASK.tif"; Mask3D="JRC2018_VNC_MALE_63x_3DMASK.nrrd";
        }else if(width==573 && height==1119 && slices==219){//573x1119x219, JRC2018 20x UNISEX
            MaskName2D="MAX_JRC2018_VNC_UNISEX_447_2DMASK.tif"; Mask3D="JRC2018_VNC_UNISEX_447_3DMASK.nrrd";
        }else if(width==572 && height==1164 && slices==229){//512x1100x220, 2017_VNC 20x MALE
            MaskName2D="MAX_JRC2018_VNC_MALE_447_G15_2DMASK.tif"; Mask3D="JRC2018_VNC_MALE_447_G15_3DMASK.nrrd";
        }else if(width==573 && height==1164 && slices==205){//512x1100x220, 2017_VNC 20x MALE
            MaskName2D="MAX_JRC2018_VNC_FEMALE_447_G15_2DMASK.tif"; Mask3D="JRC2018_VNC_FEMALE_447_G15_3DMASK.nrrd";
        }else if(width==512 && height==1100 && slices==220){//512x1100x220, 2017_VNC 20x MALE
            MaskName2D="MAX_MaleVNC2017_2DMASK.tif"; Mask3D="MaleVNC2017_3DMASK.nrrd";
            zerovalue=239907;
            zeroexi=1;
        }else if(width==512 && height==1024 && slices==220){//512x1024x220, JRC2018 20x UNISEX
            MaskName2D="MAX_FemaleVNCSymmetric2017_2DMASK.tif"; Mask3D="FemaleVNCSymmetric2017_3DMASK.nrrd";
            zerovalue=239907;
            zeroexi=1;

        }else if(width==3333 && height==1560 && slices==456){
            MaskName2D="MAX_JRC2018_UNISEX_63xOri_2DMASK.tif"; Mask3D="JRC2018_UNISEX_63xOri_3DMASK.nrrd";
        }else if(width==1652 && height==773 && slices==456){
            MaskName2D="MAX_JRC2018_UNISEX_38um_iso_2DMASK.tif"; Mask3D="JRC2018_UNISEX_38um_iso_3DMASK.nrrd";
        }else if(width==1427 && height==668 && slices==394){
            MaskName2D="MAX_JRC2018_UNISEX_40x_2DMASK.tif"; Mask3D="JRC2018_UNISEX_40x_3DMASK.nrrd";
        }else if(width==1210 && height==566 && slices==174){//1210x566x174, JRC2018 BRAIN 20xHR UNISEX
            MaskName2D="MAX_JRC2018_UNISEX_20x_HR_2DMASK.tif"; Mask3D="JRC2018_UNISEX_20x_HR_3DMASK.nrrd";
        }else if(width==1010 && height==473 && slices==174){
            MaskName2D="MAX_JRC2018_UNISEX_20x_gen1_2DMASK.tif"; Mask3D="JRC2018_UNISEX_20x_gen1_3DMASK.nrrd";

        }else if(width==3333 && height==1550 && slices==478){
            MaskName2D="MAX_JRC2018_FEMALE_63x_2DMASK.tif"; Mask3D="JRC2018_FEMALE_63x_3DMASK.nrrd";
        }else if(width==1652 && height==768 && slices==478){
            MaskName2D="MAX_JRC2018_FEMALE_38um_iso_2DMASK.tif"; Mask3D="JRC2018_FEMALE_38um_iso_3DMASK.nrrd";
        }else if(width==1427 && height==664 && slices==413){
            MaskName2D="MAX_JRC2018_FEMALE_40x_2DMASK.tif"; Mask3D="JRC2018_FEMALE_40x_3DMASK.nrrd";
        }else if(width==1210 && height==563 && slices==182){
            MaskName2D="MAX_JRC2018_FEMALE_20x_HR_2DMASK.tif"; Mask3D="JRC2018_FEMALE_20x_HR_3DMASK.nrrd";
        }else if(width==1010 && height==470 && slices==182){
            MaskName2D="MAX_JRC2018_FEMALE_20x_gen1_2DMASK.tif"; Mask3D="JRC2018_FEMALE_20x_gen1_3DMASK.nrrd";

        }else if(width==3150 && height==1500 && slices==476){//3150x1500x476, JRC2018 BRAIN 63x MALE
            MaskName2D="MAX_JRC2018_MALE_63x_2DMASK.tif"; Mask3D="JRC2018_MALE_63x_3DMASK.nrrd";
        }else if(width==1561 && height==744 && slices==476){//1561x744x476, JRC2018 BRAIN 63xDW MALE
            MaskName2D="MAX_JRC2018_MALE_38um_iso_2DMASK.tif"; Mask3D="JRC2018_MALE_38um_iso_3DMASK.nrrd";
        }else if(width==1348 && height==642 && slices==411){
            MaskName2D="MAX_JRC2018_MALE_40x_2DMASK.tif"; Mask3D="JRC2018_MALE_40x_3DMASK.nrrd";
        }else if(width==1143 && height==545 && slices==181){
            MaskName2D="MAX_JRC2018_MALE_20xHR_2DMASK.tif"; Mask3D="JRC2018_MALE_20xHR_3DMASK.nrrd";
        }else if(width==955 && height==455 && slices==181){
            MaskName2D="MAX_JRC2018_MALE_20x_gen1_2DMASK.tif"; Mask3D="JRC2018_MALE_20x_gen1_3DMASK.nrrd";

        }else if(width==1184 && height==592 && slices==218){
            MaskName2D="MAX_JFRC2013_20x_New_dist_G16_2DMASK.tif"; Mask3D="JFRC2013_20x_New_dist_G16_3DMASK.nrrd";
        }else if(width==1450 && height==725 && slices==436){
            MaskName2D="MAX_JFRC2013_63x_New_dist_G16_2DMASK.tif"; Mask3D="JFRC2013_63x_New_dist_G16_3DMASK.nrrd";

        }else if(width==1024 && height==512 && slices==220){//1024x512x220, JFRC2010
            MaskName2D="MAX_JFRC2010_2DMask.tif"; Mask3D="JFRC2010_3DMask.nrrd";
        }

        /// foreground 0 value measurement;
        int lowerweight = 0;
        if(AutoBRVST.equals("Segmentation based no lower value cut"))
            lowerweight = 0;

        //mask2Dext=File.exists(MaskDir+MaskName2D);
        File f = new File(mask2DPath);
        boolean mask2Dext = f.exists();
        long zeronumberpxPre = 0;

        Img<IntegerType> mask2D = null;
        if(!AutoBRVST.equals("Segmentation based no lower value cut")){
            if(mask2Dext){
                mask2D = ( Img<IntegerType> ) IO.openImgs(mask2DPath).get(0);
                //open(MaskDir+MaskName2D);
                zeronumberpxPre = VoxelCounter.countZeroVoxels(zProjectedImage, mask2D);
            }

            if (MaskName2D.isEmpty()){
                fillWithZeroUsingCursor(zProjectedImage, (int)Math.round(width*0.1), (int)Math.round(height*0.1), (int)Math.round(width*0.7), (int)Math.round(height*0.7));
            }

            /// background measurement, other than tissue
            long total = 0;
            if(zerovalue==0){
                Cursor<T> cursor = zProjectedImage.cursor();
                while (cursor.hasNext()) {
                    T pixel = cursor.next();
                    total = total + (long)pixel.getRealDouble();
                    if(pixel.getRealDouble() == 0.0 && zeroexi == 0)
                        zerovalue=zerovalue+1;
                }
            }

            //		zerovalue=counts[0];
            Inimin = (int)Math.round(((double)total/((height*width)-zerovalue))*0.8);//239907 is female VNC size
        }

        if(Inimin!=0 || Inimax!=65535){
            //selectWindow(stackSt);

            if(easyADJ==true || AutoBRV==1){
                ContrastEnhancer.stretchHistogram(input, 0.0, applyV, 0);
            }

            if(AutoBRVST.equals("Segmentation based no lower value cut"))
                Inimin=0;

            if(AutoBRV==1){
                ContrastEnhancer.stretchHistogram(input, 0.0, 65535, Inimin);
                //setMinAndMax(Inimin, 65535);
                //run("Apply LUT", "stack");

                long zeronumberpxPost = 0;// zero value measurement in the tissue again
                if(mask2Dext){
                    //	open(MaskDir+MaskName2D);

                    if(AutoBRVST!="Segmentation based no lower value cut"){
                        Img<T> zProjectedInput = ImageZProjection.maxIntensityProjection(input, 0);
                        zeronumberpxPost = VoxelCounter.countZeroVoxels(zProjectedInput, mask2D);
                        long gapzerovalue = zeronumberpxPost - zeronumberpxPre;
                        if(gapzerovalue > 100){
                            Inimin = 0;
                            lowerweight = 0;
                            AutoBRVST = "Segmentation based no lower value cut";
                        }
                    }//	if(AutoBRVST!="Segmentation based no lower value cut"){
                    //selectWindow(stackSt);
                }
            }
        }

        Img<T> zProjectedAdjustedInput = ImageZProjection.maxIntensityProjection(input, 15);
        long newWidth = Math.round(width*0.95);
        long newHeight = Math.round(height*0.95);
        long ox = Math.round((width - newWidth) * 0.5);
        long oy = Math.round((height - newHeight) * 0.5);
        CanvasSizeChanger.changeCanvasSize(zProjectedAdjustedInput, ox, oy, newWidth, newHeight);

        T minAdjustedT = zProjectedAdjustedInput.firstElement().createVariable();
        T maxAdjustedT = zProjectedAdjustedInput.firstElement().createVariable();
        ComputeMinMax.computeMinMax(zProjectedAdjustedInput, minAdjustedT, maxAdjustedT);
        int maxAdjusted = maxAdjustedT.getInteger();

        if(AutoBRV==0){
            if(easyADJ==false)
                applyV = 255;
            if(input.firstElement() instanceof UnsignedShortType){
                if(easyADJ==false)
                    ContrastEnhancer.scaleHistogram(input, DefMaxValue, 0, 255, 0);
                else
                    ContrastEnhancer.scaleHistogram(input, maxAdjusted, 0, 255, 0);
            }
        }//if(AutoBRV==0){


        Img<ARGBType> cdm = ColorCoder(input, slices, applyV, AutoBRV, CLAHE, colorscale, reverse0, colorcoding, usingLUT, DefMaxValue, startMIP, endMIP, expand, gammavalue, easyADJ);

        return cdm;
    }

    public static <T extends IntegerType< T >> Img<ARGBType> ColorCoder(
            Img<T> stack,
            int slicesOri,
            int applyV,
            int AutoBRV,
            boolean CLAHE,
            boolean GFrameColorScaleCheck,
            boolean reverse0,
            boolean colorcoding,
            String usingLUT,
            int DefMaxValue,
            int startMIP,
            int endMIP,
            boolean expand,
            int gammavalue,
            boolean easyADJ) {//"Time-Lapse Color Coder"

        //int[] Glut = null;
        //if(usingLUT=="royal")
        //    Glut = royal;	//default LUT

        int[] lut = psychedelicRainbow2;	//default LUT

        int Gstartf = 1;

        int width = (int)stack.dimension(0);
        int height = (int)stack.dimension(1);
        int slices = (int)stack.dimension(2);

        if (startMIP < 0) startMIP = 0;
        if (endMIP > slices || endMIP < 0) endMIP = slices;

        //rename("Original_Stack.tif"); Original_Stack -> stack

        int[] lut_table = new int[slices];

        for(int s = 0; s < slices; s++){
            double per = (double)s / slices;
            double colv = 255.0 * per;
            int val = (int)Math.round(colv);
            lut_table[s] = val;
        }

        ArrayImgFactory<ARGBType> factory = new ArrayImgFactory<>(new ARGBType());
        Img<ARGBType> cdm = factory.create(width, height);

        // Iterate over the 2D projection image
        Cursor<ARGBType> cdmCursor = cdm.cursor();
        RandomAccess<T> randomAccess = stack.randomAccess();

        while (cdmCursor.hasNext()) {
            cdmCursor.fwd();
            long x = cdmCursor.getIntPosition(0);
            long y = cdmCursor.getIntPosition(1);

            cdmCursor.get().set(0xFF000000);

            // Find the maximum intensity along the Z-axis for this x,y position
            for (int z = startMIP; z < endMIP; z++) {
                int RG1=0; int BG1=0; int GR1=0; int GB1=0; int RB1=0; int BR1=0;
                int RG2=0; int BG2=0; int GR2=0; int GB2=0; int RB2=0; int BR2=0;
                int max1=0;
                int max2=0;
                int MIPtwo=0;
                String MIPtwoST="";

                randomAccess.setPosition(x, 0);
                randomAccess.setPosition(y, 1);
                randomAccess.setPosition(z, 2);
                int val = randomAccess.get().getInteger();
                if (val > 0) {
                    int lut_r = lut[lut_table[z]*3];
                    int lut_g = lut[lut_table[z]*3 + 1];
                    int lut_b = lut[lut_table[z]*3 + 2];

                    int red1   = (int)((double)val/255.0*(double)lut_r);
                    int green1 = (int)((double)val/255.0*(double)lut_g);
                    int blue1  = (int)((double)val/255.0*(double)lut_b);

                    if(red1>blue1 && red1>green1){//RB1 & RG1
                        max1=red1;
                        if(blue1>green1){
                            RB1=red1+blue1;//1
                        }else{
                            RG1=red1+green1;//2
                        }
                    }else if(green1>blue1 && green1>red1){
                        max1=green1;
                        if(blue1>red1)
                            GB1=green1+blue1;//3
                        else
                            GR1=green1+red1;//4
                    }else if(blue1>red1 && blue1>green1){
                        max1=blue1;
                        if(red1>green1)
                            BR1=blue1+red1;//5
                        else
                            BG1=blue1+green1;//6
                    }

                    int rgb2 = cdmCursor.get().get();
                    int red2   = (rgb2>>>16) & 0xff;//MIP
                    int green2 = (rgb2>>>8) & 0xff;//MIP
                    int blue2  = rgb2 & 0xff;//MIP

                    if(red2>0 || green2>0 || blue2>0){
                        if(red2>blue2 && red2>green2){
                            max2=red2;
                            if(blue2>green2){//1
                                RB2=red2+blue2;
                                MIPtwo=RB2;
                                MIPtwoST="RB2";
                            }else{//2
                                RG2=red2+green2;
                                MIPtwo=RG2;
                                MIPtwoST="RG2";
                            }
                        }else if(green2>blue2 && green2>red2){
                            max2=green2;
                            if(blue2>red2){//3
                                GB2=green2+blue2;
                                MIPtwo=GB2;
                                MIPtwoST="GB2";
                            }else{//4
                                GR2=green2+red2;
                                MIPtwo=GR2;
                                MIPtwoST="GR2";
                            }
                        }else if(blue2>red2 && blue2>green2){
                            max2=blue2;
                            if(red2>green2){//5
                                BR2=blue2+red2;
                                MIPtwo=BR2;
                                MIPtwoST="BR2";
                            }else{//6
                                BG2=blue2+green2;
                                MIPtwo=BG2;
                                MIPtwoST="BG2";
                            }
                        }//if(red2>=blue2 && red2>=green2){

                        int rgb1 = 0;
                        if (max1 != 255 || max2 != 255) {
                            if(RB1>0){//data1 > 0
                                if(max1>max2){//1
                                    rgb1=red1;

                                    if(green2<green1)
                                        rgb1 = (rgb1 << 8) + green1;
                                    else{//green2>green1
                                        if(green2<blue1)
                                            rgb1 = (rgb1 << 8) + green2;
                                        else//if(green2>=blue1)
                                            rgb1 = (rgb1 << 8) + green1;
                                    }

                                    rgb1 = (rgb1 << 8) + blue1;
                                    cdmCursor.get().set(0xFF000000 | rgb1);
                                }else{
                                    cdmMax(cdmCursor.get(), red1, red2, green1, green2, blue1, blue2, MIPtwoST);
                                }//if(RB1<=MIPtwo){

                            }else if(RG1>0){//2

                                if(max1>max2){
                                    rgb1=red1;
                                    rgb1 = (rgb1 << 8) + green1;

                                    if(blue2<blue1)
                                        rgb1 = (rgb1 << 8) + blue1;
                                    else{//blue2>blue1
                                        if(blue2<green1)
                                            rgb1 = (rgb1 << 8) + blue2;
                                        else//(blue2>=green1)
                                            rgb1 = (rgb1 << 8) + blue1;
                                    }
                                    cdmCursor.get().set(0xFF000000 | rgb1);
                                }else{
                                    cdmMax(cdmCursor.get(), red1, red2, green1, green2, blue1, blue2, MIPtwoST);
                                }//if(RG1>MIPtwo){

                            }else if(GB1>0){//3

                                if(max1>max2){

                                    if(red2<red1)
                                        rgb1 = red1;
                                    else{//red2>red1
                                        if(red2<blue1)
                                            rgb1 = red2;
                                        else//(red2>=blue1)
                                            rgb1 =  red1;
                                    }

                                    rgb1 = (rgb1 << 8) + green1;
                                    rgb1 = (rgb1 << 8) + blue1;
                                    cdmCursor.get().set(0xFF000000 | rgb1);
                                }else{
                                    cdmMax(cdmCursor.get(), red1, red2, green1, green2, blue1, blue2, MIPtwoST);
                                }//if(RG1>MIPtwo){

                            }else if(GR1>0){//4

                                if(max1>max2){

                                    rgb1 =  red1;
                                    rgb1 = (rgb1 << 8) + green1;

                                    if(blue2<blue1)
                                        rgb1 = (rgb1 << 8) + blue1;
                                    else{//blue2>blue1
                                        if(blue2<red1)
                                            rgb1 = (rgb1 << 8) + blue2;
                                        else//(blue2>=red1)
                                            rgb1 =  (rgb1 << 8) + blue1;
                                    }
                                    cdmCursor.get().set(0xFF000000 | rgb1);
                                }else{
                                    cdmMax(cdmCursor.get(), red1, red2, green1, green2, blue1, blue2, MIPtwoST);
                                }//if(RG1>MIPtwo){

                            }else if(BR1>0){//5

                                if(max1>max2){

                                    rgb1 =  red1;

                                    if(green2<green1)
                                        rgb1 = (rgb1 << 8) + green1;
                                    else{//green2>green1
                                        if(green2<red1)
                                            rgb1 = (rgb1 << 8) + green2;
                                        else//(green2>=red1)
                                            rgb1 =  (rgb1 << 8) + green1;
                                    }

                                    rgb1 =  (rgb1 << 8) + blue1;
                                    cdmCursor.get().set(0xFF000000 | rgb1);
                                }else{
                                    cdmMax(cdmCursor.get(), red1, red2, green1, green2, blue1, blue2, MIPtwoST);
                                }//if(RG1>MIPtwo){

                            }else if(BG1>0){//6

                                if(max1>max2){

                                    if(red2<red1)
                                        rgb1 = red1;
                                    else{//red2>red1
                                        if(red2<green1)
                                            rgb1 = red2;
                                        else//(red2>=green1)
                                            rgb1 = red1;
                                    }

                                    rgb1 =  (rgb1 << 8) + green1;
                                    rgb1 =  (rgb1 << 8) + blue1;
                                    cdmCursor.get().set(0xFF000000 | rgb1);
                                }else{
                                    cdmMax(cdmCursor.get(), red1, red2, green1, green2, blue1, blue2, MIPtwoST);
                                }//if(RG1>MIPtwo){
                            }//if data1 > 0
                        }
                    }else{
                        cdmCursor.get().set(0xFF000000 | (red1 << 16) | (green1 << 8) | blue1);
                    }
                }
            }
        }
        return cdm;
    }

    public static void cdmMax(ARGBType pix, int red1, int red2, int green1, int green2, int blue1, int blue2, String MIPtwoST2){

        int rgb1 = 0;

        if(MIPtwoST2.equals("RB2")){

            rgb1 = red2;

            if(green2>green1)
                rgb1 = (rgb1 << 8) + green2;
            else{//green2<green1
                if(green1<blue2)
                    rgb1 = (rgb1 << 8) + green1;
                else//(green1>=blue2)
                    rgb1 = (rgb1 << 8) + green2;
            }

            rgb1 = (rgb1 << 8) + blue2;
            pix.set(0xFF000000 | rgb1);

        }else if(MIPtwoST2.equals("RG2")){

            rgb1 = red2;
            rgb1 = (rgb1 << 8) + green2;

            if(blue2>blue1)
                rgb1 = (rgb1 << 8) + blue2;
            else{//blue2<blue1
                if(blue1<green2)
                    rgb1 = (rgb1 << 8) + blue1;
                else//(blue1>=green2)
                    rgb1 = (rgb1 << 8) + blue2;
            }
            pix.set(0xFF000000 | rgb1);

        }else if(MIPtwoST2.equals("GB2")){

            if(red2>red1)
                rgb1 = red2;
            else{//red2<red1
                if(red1<blue2)
                    rgb1 = red1;
                else//(red1>=blue2){
                    rgb1 = red2;
            }

            rgb1 = (rgb1 << 8) + green2;
            rgb1 = (rgb1 << 8) + blue2;

            pix.set(0xFF000000 | rgb1);

        }else if(MIPtwoST2.equals("GR2")){

            rgb1 = red2;
            rgb1 = (rgb1 << 8) + green2;

            if(blue2>blue1)
                rgb1 = (rgb1 << 8) + blue2;
            else{//blue2<blue1
                if(blue1<red2)
                    rgb1 = (rgb1 << 8) + blue1;
                else//(blue1>=red2)
                    rgb1 = (rgb1 << 8) + blue2;
            }

            pix.set(0xFF000000 | rgb1);

        }else if(MIPtwoST2.equals("BR2")){

            rgb1 = red2;

            if(green2>green1)
                rgb1 = (rgb1 << 8) + green2;
            else{//green2<green1
                if(green1<red2)
                    rgb1 = (rgb1 << 8) + green1;
                else//(green1>=red2)
                    rgb1 = (rgb1 << 8) + green2;
            }

            rgb1 = (rgb1 << 8) + blue2;

            pix.set(0xFF000000 | rgb1);

        }else if(MIPtwoST2.equals("BG2")){

            if(red2>red1)
                rgb1 = red2;
            else{//red2<red1
                if(red1<green2)
                    rgb1 = red1;
                else//(red1>=green2)
                    rgb1 = red2;
            }

            rgb1 = (rgb1 << 8) + green2;
            rgb1 = (rgb1 << 8) + blue2;

            pix.set(0xFF000000 | rgb1);
        }
    }

    public <T extends IntegerType< T >> Img<T> ThreeDconnect_component (Img<T> input){
        Img<T> segments = ConnectingComponent3D.run(input, 25, 300);
        Img<T> largestSegment = ImageThresholding.createBinaryImage(segments, 1, 1);
        Img<T> result = ImageANDOperation.andOperation(input, largestSegment);

        return result;
    }

    static final int[] psychedelicRainbow2 = {
            127,0,255,
            125,3,255,
            124,6,255,
            122,9,255,
            121,12,255,
            120,15,255,
            119,18,255,
            118,21,255,
            116,24,255,
            115,27,255,
            114,30,255,
            113,33,255,
            112,36,255,
            110,39,255,
            109,42,255,
            108,45,255,
            106,48,255,
            105,51,255,
            104,54,255,
            103,57,255,
            101,60,255,
            100,63,255,
            99,66,255,
            98,69,255,
            96,72,255,
            95,75,255,
            94,78,255,
            93,81,255,
            92,84,255,
            90,87,255,
            89,90,255,
            87,93,255,
            86,96,255,
            84,99,255,
            83,102,255,
            81,105,255,
            80,108,255,
            78,111,255,
            77,114,255,
            75,117,255,
            74,120,255,
            72,123,255,
            71,126,255,
            69,129,255,
            68,132,255,
            66,135,255,
            65,138,255,
            63,141,255,
            62,144,255,
            60,147,255,
            59,150,255,
            57,153,255,
            56,156,255,
            54,159,255,
            53,162,255,
            51,165,255,
            50,168,255,
            48,171,255,
            47,174,255,
            45,177,255,
            44,180,255,
            42,183,255,
            41,186,255,
            39,189,255,
            38,192,255,
            36,195,255,
            35,198,255,
            33,201,255,
            32,204,255,
            30,207,255,
            29,210,255,
            27,213,255,
            26,216,255,
            24,219,255,
            23,222,255,
            21,225,255,
            20,228,255,
            18,231,255,
            16,234,255,
            14,237,255,
            12,240,255,
            9,243,255,
            6,246,255,
            3,249,255,
            1,252,255,
            0,254,255,
            3,255,252,
            6,255,249,
            9,255,246,
            12,255,243,
            15,255,240,
            18,255,237,
            21,255,234,
            24,255,231,
            27,255,228,
            30,255,225,
            33,255,222,
            36,255,219,
            39,255,216,
            42,255,213,
            45,255,210,
            48,255,207,
            51,255,204,
            54,255,201,
            57,255,198,
            60,255,195,
            63,255,192,
            66,255,189,
            69,255,186,
            72,255,183,
            75,255,180,
            78,255,177,
            81,255,174,
            84,255,171,
            87,255,168,
            90,255,165,
            93,255,162,
            96,255,159,
            99,255,156,
            102,255,153,
            105,255,150,
            108,255,147,
            111,255,144,
            114,255,141,
            117,255,138,
            120,255,135,
            123,255,132,
            126,255,129,
            129,255,126,
            132,255,123,
            135,255,120,
            138,255,117,
            141,255,114,
            144,255,111,
            147,255,108,
            150,255,105,
            153,255,102,
            156,255,99,
            159,255,96,
            162,255,93,
            165,255,90,
            168,255,87,
            171,255,84,
            174,255,81,
            177,255,78,
            180,255,75,
            183,255,72,
            186,255,69,
            189,255,66,
            192,255,63,
            195,255,60,
            198,255,57,
            201,255,54,
            204,255,51,
            207,255,48,
            210,255,45,
            213,255,42,
            216,255,39,
            219,255,36,
            222,255,33,
            225,255,30,
            228,255,27,
            231,255,24,
            234,255,21,
            237,255,18,
            240,255,15,
            243,255,12,
            246,255,9,
            249,255,6,
            252,255,3,
            254,255,0,
            255,252,3,
            255,249,6,
            255,246,9,
            255,243,12,
            255,240,15,
            255,237,18,
            255,234,21,
            255,231,24,
            255,228,27,
            255,225,30,
            255,222,33,
            255,219,36,
            255,216,39,
            255,213,42,
            255,210,45,
            255,207,48,
            255,204,51,
            255,201,54,
            255,198,57,
            255,195,60,
            255,192,63,
            255,189,66,
            255,186,69,
            255,183,72,
            255,180,75,
            255,177,78,
            255,174,81,
            255,171,84,
            255,168,87,
            255,165,90,
            255,162,93,
            255,159,96,
            255,156,99,
            255,153,102,
            255,150,105,
            255,147,108,
            255,144,111,
            255,141,114,
            255,138,117,
            255,135,120,
            255,132,123,
            255,129,126,
            255,126,129,
            255,123,132,
            255,120,135,
            255,117,138,
            255,114,141,
            255,111,144,
            255,108,147,
            255,105,150,
            255,102,153,
            255,99,156,
            255,96,159,
            255,93,162,
            255,90,165,
            255,87,168,
            255,84,171,
            255,81,173,
            255,78,174,
            255,75,175,
            255,72,176,
            255,69,177,
            255,66,178,
            255,63,179,
            255,60,180,
            255,57,181,
            255,54,182,
            255,51,183,
            255,48,184,
            255,45,185,
            255,42,186,
            255,39,187,
            255,36,188,
            255,33,189,
            255,30,190,
            255,27,191,
            255,24,192,
            255,21,193,
            255,18,194,
            255,15,195,
            255,12,196,
            255,9,197,
            255,6,198,
            255,3,199,
            255,0,200
    };
}
