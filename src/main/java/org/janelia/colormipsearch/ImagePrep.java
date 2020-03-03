package org.janelia.colormipsearch;

import java.awt.Color;
import java.awt.Font;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import ij.IJ;
import ij.ImagePlus;
import ij.ImageStack;
import ij.process.ImageConverter;
import ij.process.ImageProcessor;

public class ImagePrep {

    ImagePlus CDM_area_measure(ImagePlus impstack, ImagePlus impmask, final String gradientDIR, final boolean rungradientonthefly, final int thresmEf, final int maxnumberF, final boolean mirror_maskEF, boolean showFlipF) {

        ImagePlus newimp = null;

        int stackslicenum = 0;

        int[] info = impstack.getDimensions();
        stackslicenum = info[3]; // 52

        final ImageStack originalresultstack = impstack.getStack();
        long startT = System.currentTimeMillis();

        // perform maskBodyIdLabel

        int[] fliparray = new int[stackslicenum];
        long[] areaarray = new long[stackslicenum];

        // original area measurement////////////////////
        String maskname = impmask.getTitle();

        ImagePlus gradientMaskImage = impmask.duplicate();
        ImagePlus max10pxRGBMaskImage = impmask.duplicate();

        // convert the mask to gray8
        ImageConverter ic = new ImageConverter(gradientMaskImage);
        ic.convertToGray8();

        ImageProcessor gradientMaskProcessor = gradientMaskImage.getProcessor(); // this will be binarized mask with all mask pixels bellow the threshold turned off
        ImageProcessor max10pxRGBMaskProcessor = max10pxRGBMaskImage.getProcessor(); // this image will have all pixels that have R, G, and B bellow the threshold turned off (black)

        info = gradientMaskImage.getDimensions();
        int WW = info[0];
        int HH = info[1];
        final int sumpx = WW * HH;
        for (int ipix = 0; ipix < sumpx; ipix++) { // 255 binary mask creation
            // binarize the gray8 mask
            if (gradientMaskProcessor.get(ipix) > thresmEf) {
                gradientMaskProcessor.set(ipix, 255);
            } else
                gradientMaskProcessor.set(ipix, 0);

            int rgbPix = max10pxRGBMaskProcessor.get(ipix);

            int redval = (rgbPix >>> 16) & 0xff;
            int greenval = (rgbPix >>> 8) & 0xff;
            int blueval = rgbPix & 0xff;

            if (redval <= thresmEf && greenval <= thresmEf && blueval <= thresmEf)
                max10pxRGBMaskProcessor.set(ipix, -16777216);

        }

        IJ.run(gradientMaskImage, "Max Filter2D", "expansion=10 cpu=1 xyscaling=1");

        IJ.run(max10pxRGBMaskImage, "Maximum...", "radius=10");


        //// open gradient mask ////////////////////////////////
        ImagePlus hemiMIPmask;

        File hemiMIPFName = new File(gradientDIR, "MAX_hemi_to_JRC2018U_fineTune.png");
        if (!hemiMIPFName.exists()) {
            return gradientMaskImage;
        } else {
            hemiMIPmask = IJ.openImage(hemiMIPFName.getAbsolutePath());
        }

        ImageProcessor iphemiMIP = hemiMIPmask.getProcessor();

        /// make flipped mask 10px dilated image ////////////////////////////
        ImagePlus flippedGradientMaskImage = gradientMaskImage.duplicate();
        ImageProcessor flippedGradientMaskProcessor = flippedGradientMaskImage.getProcessor();
        ImagePlus flippedMax10pxRGBMaskImage = max10pxRGBMaskImage.duplicate();
        ImageProcessor flippedMax10pxRGBMaskProcessor = flippedMax10pxRGBMaskImage.getProcessor();

        if (mirror_maskEF) {
            flippedGradientMaskProcessor.flipHorizontal();
            flippedMax10pxRGBMaskProcessor.flipHorizontal();// flipped RGBmask

            gradientMaskProcessor = deleteOutSideMask(gradientMaskProcessor, iphemiMIP); // delete out side of emptyhemibrain region
            flippedGradientMaskProcessor = deleteOutSideMask(flippedGradientMaskProcessor, iphemiMIP); // Flipped mask; delete out side of emptyhemibrain region

            max10pxRGBMaskProcessor = deleteOutSideMask(max10pxRGBMaskProcessor, iphemiMIP); // RGBmask; delete out side of EM mask
            flippedMax10pxRGBMaskProcessor = deleteOutSideMask(flippedMax10pxRGBMaskProcessor, iphemiMIP); // flipped RGBmask; delete out side of EM mask

            ic = new ImageConverter(gradientMaskImage);
            ic.convertToGray16();
            gradientMaskProcessor = gradientMaskImage.getProcessor();

            ImageConverter ic2 = new ImageConverter(flippedGradientMaskImage);
            ic2.convertToGray16();
            flippedGradientMaskProcessor = flippedGradientMaskImage.getProcessor();


            for (int ipix = 0; ipix < sumpx; ipix++) {// 255 binary mask creation, posi signal become 0 INV
                if (gradientMaskProcessor.get(ipix) < 1)
                    gradientMaskProcessor.set(ipix, 65535);
                else
                    gradientMaskProcessor.set(ipix, 0);

                if (flippedGradientMaskProcessor.get(ipix) < 1)
                    flippedGradientMaskProcessor.set(ipix, 65535);
                else
                    flippedGradientMaskProcessor.set(ipix, 0);
            }

            flippedGradientMaskProcessor = gradientslice(flippedGradientMaskProcessor); // make Flip gradient mask, Flipped ver of

        } else {//	if(mirror_maskEF){
            // invert the binarized mask
            for (int ipix = 0; ipix < sumpx; ipix++) {// 255 binary mask creation, posi signal become 0 INV
                if (gradientMaskProcessor.get(ipix) < 1)
                    gradientMaskProcessor.set(ipix, 65535);
                else
                    gradientMaskProcessor.set(ipix, 0);

            }
        }
        gradientMaskProcessor = gradientslice(gradientMaskProcessor); // make gradient mask, EightIMG is 0 center & gradient mask of original mask

        final ImagePlus impRGBMaskFlipfinal = flippedMax10pxRGBMaskImage;
        final ImageProcessor ipgradientFlipMaskFinal = flippedGradientMaskProcessor;
        gradientMaskImage.hide();

        ImagePlus gray16MaskIMP = impmask.duplicate();

        ic = new ImageConverter(gray16MaskIMP);
        ic.convertToGray16();

        gray16MaskIMP = setsignal1(gray16MaskIMP, sumpx);

        final ImageProcessor gray16MaskProcessor = gray16MaskIMP.getProcessor();

        ImagePlus flippedGray16MaskImage = gray16MaskIMP.duplicate();
        ImageProcessor flippedGray16MaskImageProcessor = flippedGray16MaskImage.getProcessor();

        if (mirror_maskEF) {
            flippedGray16MaskImageProcessor.flipHorizontal();
            flippedGray16MaskImageProcessor = deleteOutSideMask(flippedGray16MaskImageProcessor, iphemiMIP); // delete out side of emptyhemibrain region
        }

        hemiMIPmask.unlock();
        hemiMIPmask.close();

        int PositiveStackSlicePre = stackslicenum;

        if (PositiveStackSlicePre > maxnumberF + 50)
            PositiveStackSlicePre = maxnumberF + 50;

        final int PositiveStackSlice = PositiveStackSlicePre;

        String[] namearray = new String[PositiveStackSlice];
        String[] totalnamearray = new String[PositiveStackSlice];
        double[] scorearray = new double[PositiveStackSlice];

        final String[] gaparray = new String[PositiveStackSlice];

        double maxScore = 0;
        long maxAreagap = 0;
        ImageStack Stack2stack;

        /// name array creation ///////////////////
        for (int iname = 1; iname <= PositiveStackSlice; iname++) {

            namearray[iname - 1] = originalresultstack.getSliceLabel(iname);

            int spaceIndex = namearray[iname - 1].indexOf(" ");
            if (spaceIndex != -1) {// replace slice label
                namearray[iname - 1] = namearray[iname - 1].replace(" ", "_");
                originalresultstack.setSliceLabel(namearray[iname - 1], iname);
            }

            int undeIndex = namearray[iname - 1].indexOf("_");

            scorearray[iname - 1] = Double.parseDouble(namearray[iname - 1].substring(0, undeIndex));

            if (maxScore < scorearray[iname - 1])
                maxScore = scorearray[iname - 1];

        }//for(int iname=0; iname<PositiveStackSlice; iname++){

        if (rungradientonthefly == true) {
            ImagePlus gray8InputStack = impstack.duplicate();

            ImageConverter ic3 = new ImageConverter(gray8InputStack);
            ic3.convertToGray8();

            Stack2stack = gray8InputStack.getStack();

            for (int istackscan = 1; istackscan <= PositiveStackSlice; istackscan++) {

                ImageProcessor EightStackIMG = Stack2stack.getProcessor(istackscan);

                for (int ipix2 = 0; ipix2 < sumpx; ipix2++) {// 255 binary mask creation
                    if (EightStackIMG.get(ipix2) > 1) {
                        EightStackIMG.set(ipix2, 255);
                    }
                }
            }

            IJ.run(gray8InputStack, "Max Filter2D", "expansion=10 cpu=15 xyscaling=1");

            ic = new ImageConverter(gray8InputStack);
            ic.convertToGray16();

            Stack2stack = gray8InputStack.getStack();

            for (int istackscan2 = 1; istackscan2 <= PositiveStackSlice; istackscan2++) {
                ImageProcessor stackgradientIP = Stack2stack.getProcessor(istackscan2);

                for (int ipix = 0; ipix < sumpx; ipix++) {// 255 binary mask creation
                    if (stackgradientIP.get(ipix) == 0)
                        stackgradientIP.set(ipix, 65535);
                    else
                        stackgradientIP.set(ipix, 0);
                }
            }//for(int istackscan2=1; istackscan2<=150; istackscan2++){

            long timestart = System.currentTimeMillis();
            IJ.run(gray8InputStack, "Mask Outer Gradient Stack", "");

            gray8InputStack.hide();
            Stack2stack = gray8InputStack.getStack();

            long timeend = System.currentTimeMillis();

            long gapS = (timeend - timestart) / 1000;
            IJ.log("Gradient creation; " + gapS + " second");

        } else//if(rungradientonthefly==1){
            Stack2stack = impstack.getStack();

        final ImageStack Stack2stackFinal = Stack2stack;

        IJ.log("stackslicenum; " + stackslicenum + "  PositiveStackSlice; " + PositiveStackSlice);
        long startTRGB = System.currentTimeMillis();
        final AtomicInteger ai = new AtomicInteger(1);
        final Thread[] threads = new Thread[20];

        final ImageProcessor flippedGray16MaskImageProcessorFinal = flippedGray16MaskImageProcessor;
        final ImagePlus max10pxRGBMaskImageFinal = max10pxRGBMaskImage;

        final ImagePlus gradientMaskImageFinal = gradientMaskImage;

        for (int ithread = 0; ithread < threads.length; ithread++) {
            // Concurrently run in as many threads as CPUs
            threads[ithread] = new Thread() {

                {
                    setPriority(Thread.NORM_PRIORITY);
                }

                public void run() {

                    for (int isli = ai.getAndIncrement(); isli < PositiveStackSlice + 1; isli = ai.getAndIncrement()) {

                        ImageProcessor ipresult = originalresultstack.getProcessor(isli);
                        ImagePlus SLICEtifimp = new ImagePlus("SLICE.tif", ipresult);

                        // multipy image creation/////////////
                        ImageConverter ic2 = new ImageConverter(SLICEtifimp);
                        ic2.convertToGray16();

                        SLICEtifimp = setsignal1(SLICEtifimp, sumpx);

                        ImageProcessor SLICEtifip = SLICEtifimp.getProcessor();
                        ImageProcessor ipgradientMask = gradientMaskImageFinal.getProcessor();

                        for (int ivx = 0; ivx < sumpx; ivx++) {

                            int pix1 = SLICEtifip.get(ivx);
                            int pix2 = ipgradientMask.get(ivx);

                            SLICEtifip.set(ivx, pix1 * pix2);

                        }// multiply slice and gradient mask

                        ImagePlus impOriStackResult = new ImagePlus("impOriStackResult.tif", originalresultstack.getProcessor(isli));// original stack slice

                        SLICEtifimp = deleteMatchZandCreateZnegativeScoreIMG(SLICEtifimp, impOriStackResult, max10pxRGBMaskImageFinal, sumpx);

                        long MaskToSample = sumPXmeasure(SLICEtifimp);

                        SLICEtifimp.unlock();
                        SLICEtifimp.close();

                        long MaskToSampleFlip = 0;
                        //// flip ////////////////////////////////////
                        if (mirror_maskEF) {
                            ImagePlus SLICEtifimpFlip = new ImagePlus("SLICEflip.tif", ipresult);// original stack slice
                            ic2 = new ImageConverter(SLICEtifimpFlip);
                            ic2.convertToGray16();

                            setsignal1(SLICEtifimpFlip, sumpx);
                            SLICEtifip = SLICEtifimpFlip.getProcessor();


                            for (int ivx2 = 0; ivx2 < sumpx; ivx2++) {

                                int pix1 = SLICEtifip.get(ivx2);
                                int pix2 = ipgradientFlipMaskFinal.get(ivx2);

                                SLICEtifip.set(ivx2, pix1 * pix2);

                            }// multiply slice and gradient mask

                            SLICEtifimpFlip = deleteMatchZandCreateZnegativeScoreIMG(SLICEtifimpFlip, impOriStackResult, impRGBMaskFlipfinal, sumpx);

                            MaskToSampleFlip = sumPXmeasure(SLICEtifimpFlip);

                            SLICEtifimpFlip.unlock();
                            SLICEtifimpFlip.hide();
                            SLICEtifimpFlip.close();
                        }//	if(flip==1){


                        /// just for initialization of impgradient /////////////////////////
                        ImageProcessor ipSLICE2 = Stack2stackFinal.getProcessor(isli);// already gradient stack
                        String titleslice = Stack2stackFinal.getSliceLabel(isli);

                        ImagePlus impgradient = new ImagePlus(titleslice, ipSLICE2);
                        /////////////end

                        if (rungradientonthefly == false) {

                            int undeindex = namearray[isli - 1].indexOf("_");
                            String filename = namearray[isli - 1].substring(undeindex + 1, namearray[isli - 1].length());

                            if (filename.endsWith(".tif"))
                                filename = filename.replace(".tif", ".png");

                            File f = new File(gradientDIR + filename);
                            if (!f.exists()) {
                                IJ.log("The file cannot open; " + gradientDIR + filename);
                                return;
                            } else {
                                impgradient = IJ.openImage(gradientDIR + filename);
                            }
                        }//if(rungradientonthefly==false){

                        ImagePlus impSLICE2 = impgradient.duplicate();

                        ImageProcessor ipforfunc2 = impSLICE2.getProcessor();

                        for (int ivx2 = 0; ivx2 < sumpx; ivx2++) {//multiply images
                            int pix1 = ipforfunc2.get(ivx2);
                            int pix2 = gray16MaskProcessor.get(ivx2);

                            ipforfunc2.set(ivx2, pix1 * pix2);
                        }// multiply slice and gradient mask

                        impSLICE2 = deleteMatchZandCreateZnegativeScoreIMG(impSLICE2, impOriStackResult, max10pxRGBMaskImageFinal, sumpx);

                        long SampleToMask = sumPXmeasure(impSLICE2);


                        impSLICE2.unlock();
                        impSLICE2.hide();
                        impSLICE2.close();
                        long SampleToMaskflip;
                        long normalval = (SampleToMask + MaskToSample) / 2;
                        long realval = normalval;
                        int fliphit = 0;

                        if (mirror_maskEF) {
                            //// flip X//////////////////////////////////////
                            ImagePlus impSLICE2F = impgradient.duplicate();//single slice from em bodyID hit

                            ImageProcessor ipforfunc21 = impSLICE2F.getProcessor();

                            for (int ivx2 = 0; ivx2 < sumpx; ivx2++) {

                                int pix1 = ipforfunc21.get(ivx2);
                                int pix2 = flippedGray16MaskImageProcessorFinal.get(ivx2);

                                ipforfunc21.set(ivx2, pix1 * pix2);
                            }// multiply slice and gradient mask

                            SampleToMaskflip = sumPXmeasure(impSLICE2F);

                            impSLICE2F.unlock();
                            impSLICE2F.close();

                            flippedGray16MaskImage.unlock();
                            flippedGray16MaskImage.close();

                            long flipval = (SampleToMaskflip + MaskToSampleFlip) / 2;
                            if (flipval <= normalval) {
                                realval = flipval;
                                fliparray[isli - 1] = 1;
                            }
                        }//	if(flip==1){

                        areaarray[isli - 1] = realval;

                        impgradient.unlock();
                        impgradient.close();

                    }//2385 for(int isli=1; isli<=slices; isli++){
                }
            };
        }//	for (int ithread = 0; ithread < threads.length; ithread++) {
        startAndJoin(threads);

        flippedMax10pxRGBMaskImage.unlock();
        flippedMax10pxRGBMaskImage.close();

        for (int iscorescan = 0; iscorescan < PositiveStackSlice; iscorescan++) {
            if (maxAreagap < areaarray[iscorescan])
                maxAreagap = areaarray[iscorescan];
        }

        long endTRGB = System.currentTimeMillis();
        long gapT2 = endTRGB - startTRGB;

        IJ.log(gapT2 / 1000 + " sec for 2D distance score & RGB 3D score generation");

        gradientMaskImage.unlock();
        gradientMaskImage.close();

        gradientMaskImageFinal.unlock();
        gradientMaskImageFinal.close();

        max10pxRGBMaskImage.unlock();
        max10pxRGBMaskImage.close();

        double[] normScorePercent = new double[PositiveStackSlice];
        /// normalize score ////////////////////
        for (int inorm = 0; inorm < PositiveStackSlice; inorm++) {

            double normAreaPercent = (double) areaarray[inorm] / (double) maxAreagap;
            normScorePercent[inorm] = scorearray[inorm] / maxScore;

            normAreaPercent = normAreaPercent * 2;
            if (normAreaPercent > 1)
                normAreaPercent = 1;

            if (normAreaPercent < 0.002)
                normAreaPercent = 0.002;

            double doubleGap = (normScorePercent[inorm] / normAreaPercent) * 100;

            String addST = "_";
            if (doubleGap < 100000 && doubleGap > 9999.999999)
                addST = addST.replace("_", "0");
            else if (doubleGap < 10000 && doubleGap > 999.999999)
                addST = addST.replace("_", "00");
            else if (doubleGap < 1000 && doubleGap > 99.999999)
                addST = addST.replace("_", "000");
            else if (doubleGap < 100 && doubleGap > 9.999999)
                addST = addST.replace("_", "0000");
            else if (doubleGap < 10)
                addST = addST.replace("_", "00000");

            String finalpercent = String.format("%.10f", (normScorePercent[inorm] / normAreaPercent) * 100);

            gaparray[inorm] = addST.concat(finalpercent);//,10

            String S1 = gaparray[inorm].concat(" ");

            totalnamearray[inorm] = S1.concat(namearray[inorm]);
        }

        Arrays.sort(gaparray, Collections.reverseOrder());

        int Finslice = PositiveStackSlice;
        if (Finslice > maxnumberF)
            Finslice = maxnumberF;

        ImageStack Stackfinal = new ImageStack(WW, HH);

        for (int inew = 0; inew < Finslice; inew++) {

            double Totalscore = Double.parseDouble(gaparray[inew]);
            String slicename = "";

            for (int iscan = 0; iscan < totalnamearray.length; iscan++) {
                String[] arg2 = totalnamearray[iscan].split(" ");

                double arg2_0 = Double.parseDouble(arg2[0]);

                if (arg2_0 == Totalscore) {
                    slicename = arg2[1];
                    arg2_0 = 0;
                    totalnamearray[iscan] = String.valueOf(arg2[0]) + " " + arg2[1];
                    iscan = totalnamearray.length;
                }
            }

            for (int searchS = 1; searchS <= PositiveStackSlice; searchS++) {
                String slititle = namearray[searchS - 1];
                String ADD0 = "0";
                if (inew < 10)
                    ADD0 = "00";
                else if (inew > 99)
                    ADD0 = "";

                if (slititle.equals(slicename)) {
                    ImageProcessor hitslice = originalresultstack.getProcessor(searchS);//original search MIP stack

                    if (fliparray[searchS - 1] == 1 && showFlipF == true) {
                        hitslice.flipHorizontal();

                        hitslice.setFont(new Font("SansSerif", Font.PLAIN, 26));
                        hitslice.drawString("Flip", WW / 2 - 24, 40, Color.white);
                    }

                    Stackfinal.addSlice(ADD0 + inew + "_" + gaparray[inew].substring(0, gaparray[inew].indexOf(".")) + "_" + slititle, hitslice);

                    searchS = PositiveStackSlice + 1;
                }//if(slititle==slicename){
            }//for(searchS=1; seachS<nSlices; searchS++){
        }//for(int inew=0; inew<Finslice; inew++){

        gray16MaskIMP.unlock();
        gray16MaskIMP.hide();
        gray16MaskIMP.close();
        flippedGradientMaskImage.unlock();
        flippedGradientMaskImage.hide();
        flippedGradientMaskImage.close();

        newimp = new ImagePlus("Search_Result" + maskname, Stackfinal);
        newimp.show();
        long endT = System.currentTimeMillis();
        long gapT = endT - startT;

        return newimp;
    }//public class CDM_area_measure

    private long sumPXmeasure(ImagePlus ipmf) {
        int morethan = 3;
        ImageProcessor ip2;

        ip2 = ipmf.getProcessor();

        int sumpx = ip2.getPixelCount();

        long sumvalue = 0;
        for (int i = 0; i < sumpx; i++) {
            int pix = ip2.get(i);

            if (pix > morethan) {
                sumvalue = sumvalue + pix;
            }
        }

        return sumvalue;
    }

    private ImagePlus setsignal1(ImagePlus impfunc, int sumpxfunc) {
        for (int iff = 0; iff < sumpxfunc; iff++) {
            ImageProcessor ipfunc = impfunc.getProcessor();
            int pix = ipfunc.get(iff);    //input

            if (pix > 2)
                ipfunc.set(iff, 1);//out put

        }
        impfunc.unlock();
        impfunc.close();
        return impfunc;
    }

    private ImageProcessor gradientslice(ImageProcessor ipgra) {

        int nextmin = 0;
        int Stop = 0;
        int Pout = 0;

        ImagePlus impfunc = new ImagePlus("funIMP", ipgra);

        int Fmaxvalue = 255;
        if (impfunc.getType() == impfunc.GRAY8)
            Fmaxvalue = 255;
        else if (impfunc.getType() == impfunc.GRAY16)
            Fmaxvalue = 65535;

        int[] infof = impfunc.getDimensions();
        int width = infof[0];
        int height = infof[1];

        while (Stop == 0) {

            Stop = 1;
            Pout = Pout + 1;

            for (int ix = 0; ix < width; ix++) {// x pixel shift
                for (int iy = 0; iy < height; iy++) {

                    int pix0 = -1;
                    int pix1 = -1;
                    int pix2 = -1;
                    int pix3 = -1;

                    int pix = ipgra.get(ix, iy);

                    if (pix == Fmaxvalue) {

                        if (ix > 0)
                            pix0 = ipgra.get(ix - 1, iy);

                        if (ix < width - 1)
                            pix1 = ipgra.get(ix + 1, iy);

                        if (iy > 0)
                            pix2 = ipgra.get(ix, iy - 1);

                        if (iy < height - 1)
                            pix3 = ipgra.get(ix, iy + 1);

                        if (pix0 == nextmin || pix1 == nextmin || pix2 == nextmin || pix3 == nextmin) {//|| edgepositive==1

                            ipgra.set(ix, iy, Pout);
                            Stop = 0;
                        }
                    }//if(pix==Fmaxvalue){

                }//	for(int iy=0; iy<height; iy++){
            }//	for(int ix=0; ix<width; ix++){// x pixel shift
            nextmin = nextmin + 1;
        }//	while(Stop==0){
        impfunc.unlock();
        impfunc.close();
        return ipgra;
    }

    private ImageProcessor deleteOutSideMask(ImageProcessor ipneuron, ImageProcessor ipEMmask) {

        ImagePlus impneuron = new ImagePlus("neuronIMP", ipneuron);

        int[] infof = impneuron.getDimensions();
        int width = infof[0];
        int height = infof[1];
        int sumpxf = width * height;
        for (int idel = 0; idel < sumpxf; idel++) {
            int neuronpix = ipneuron.get(idel);
            int maskpix = ipEMmask.get(idel);
            if (impneuron.getType() != ImagePlus.COLOR_RGB) {
                if (neuronpix > 0) {
                    if (maskpix == 0) {
                        ipneuron.set(idel, 0);
                    }
                }
            } else {
                if (neuronpix != -16777216) {
                    if (maskpix == 0) {
                        ipneuron.set(idel, -16777216);
                    }
                }
            }
        }

        impneuron.unlock();
        impneuron.close();

        return ipneuron;
    }

    private ImagePlus deleteMatchZandCreateZnegativeScoreIMG(ImagePlus SLICEtifimpF, ImagePlus impOriStackResultF, ImagePlus imp10pxRGBmaskF, int sumpxF) {
        // SLICEtifipF is score gradient 16bit image
        //IPOriStackResultF is original RGBimage from 3D hit stack
        //IP10pxRGBmaskF is dilated RGB mask

        ImageProcessor SLICEtifipF = SLICEtifimpF.getProcessor();
        ImageProcessor IPOriStackResultF = impOriStackResultF.getProcessor();
        ImageProcessor IP10pxRGBmaskF = imp10pxRGBmaskF.getProcessor();

        int colorFlux = 40; // color fluctuation of matching, 5 microns

        for (int icompare = 0; icompare < sumpxF; icompare++) {

            int dilatedmaskpix = IP10pxRGBmaskF.get(icompare);

            if (dilatedmaskpix != -16777216) {// if 10px expand RGBmask is not 0

                int orimaskpix = IPOriStackResultF.get(icompare);

                if (orimaskpix != -16777216) {// / if original RGBmask is not 0
                    int redDileate = (dilatedmaskpix >>> 16) & 0xff;
                    int greenDileate = (dilatedmaskpix >>> 8) & 0xff;
                    int blueDileate = dilatedmaskpix & 0xff;


                    int red1 = (orimaskpix >>> 16) & 0xff;
                    int green1 = (orimaskpix >>> 8) & 0xff;
                    int blue1 = orimaskpix & 0xff;


                    int pxGapSlice = calc_slicegap_px(red1, green1, blue1, redDileate, greenDileate, blueDileate);

                    if (colorFlux <= pxGapSlice - colorFlux) {// set negative score value to gradient SLICEtifipF
                        SLICEtifipF.set(icompare, pxGapSlice - colorFlux);
                    }
                }
            }//if(dilatedmaskpix!=-16777216){// if mask is not 0 RGB

        }//for(int icompare=0; icompare<sumpxF; icompare++){

        imp10pxRGBmaskF.unlock();
        imp10pxRGBmaskF.close();

        impOriStackResultF.unlock();
        impOriStackResultF.close();

        SLICEtifimpF.unlock();
        SLICEtifimpF.close();

        return SLICEtifimpF;
    }

    private int calc_slicegap_px(int red1, int green1, int blue1, int red2, int green2, int blue2) {

        int max1stvalMASK = 0, max2ndvalMASK = 0, max1stvalDATA = 0, max2ndvalDATA = 0, maskslinumber = 0, dataslinumber = 0;
        String mask1stMaxColor = "Black", mask2ndMaxColor = "Black", data1stMaxColor = "Black", data2ndMaxColor = "Black";

        if (red1 >= green1 && red1 >= blue1) {
            max1stvalMASK = red1;
            mask1stMaxColor = "red";
            if (green1 >= blue1) {
                max2ndvalMASK = green1;
                mask2ndMaxColor = "green";
            } else {
                max2ndvalMASK = blue1;
                mask2ndMaxColor = "blue";
            }
        } else if (green1 >= red1 && green1 >= blue1) {
            max1stvalMASK = green1;
            mask1stMaxColor = "green";
            if (red1 >= blue1) {
                mask2ndMaxColor = "red";
                max2ndvalMASK = red1;
            } else {
                max2ndvalMASK = blue1;
                mask2ndMaxColor = "blue";
            }
        } else if (blue1 >= red1 && blue1 >= green1) {
            max1stvalMASK = blue1;
            mask1stMaxColor = "blue";
            if (red1 >= green1) {
                max2ndvalMASK = red1;
                mask2ndMaxColor = "red";
            } else {
                max2ndvalMASK = green1;
                mask2ndMaxColor = "green";
            }
        }

        if (red2 >= green2 && red2 >= blue2) {
            max1stvalDATA = red2;
            data1stMaxColor = "red";
            if (green2 >= blue2) {
                max2ndvalDATA = green2;
                data2ndMaxColor = "green";
            } else {
                max2ndvalDATA = blue2;
                data2ndMaxColor = "blue";
            }
        } else if (green2 >= red2 && green2 >= blue2) {
            max1stvalDATA = green2;
            data1stMaxColor = "green";
            if (red2 >= blue2) {
                max2ndvalDATA = red2;
                data2ndMaxColor = "red";
            } else {
                max2ndvalDATA = blue2;
                data2ndMaxColor = "blue";
            }
        } else if (blue2 >= red2 && blue2 >= green2) {
            max1stvalDATA = blue2;
            data1stMaxColor = "blue";
            if (red2 >= green2) {
                max2ndvalDATA = red2;
                data2ndMaxColor = "red";
            } else {
                max2ndvalDATA = green2;
                data2ndMaxColor = "green";
            }
        }

        double maskratio = (double) max2ndvalMASK / (double) max1stvalMASK;
        double dataratio = (double) max2ndvalDATA / (double) max1stvalDATA;
        if (mask1stMaxColor.equals("red")) {//cheking slice num 172-256
            if (mask2ndMaxColor.equals("green"))//172-213
                maskslinumber = calc_slicenumber(171, 212, maskratio);
            if (mask2ndMaxColor.equals("blue"))//214-256
                maskslinumber = calc_slicenumber(213, 255, maskratio);
        } else if (mask1stMaxColor.equals("green")) {//cheking slice num 87-171
            if (mask2ndMaxColor.equals("red"))//129-171
                maskslinumber = calc_slicenumber(128, 170, maskratio);
            if (mask2ndMaxColor.equals("blue"))//87-128
                maskslinumber = calc_slicenumber(86, 127, maskratio);
        } else if (mask1stMaxColor.equals("blue")) {//cheking slice num 1-86 = 0-85
            if (mask2ndMaxColor.equals("red"))//1-30
                maskslinumber = calc_slicenumber(0, 29, maskratio);
            if (mask2ndMaxColor.equals("green"))//31-86
                maskslinumber = calc_slicenumber(30, 85, maskratio);
        }


        if (data1stMaxColor.equals("red")) {//cheking slice num 172-256
            if (data2ndMaxColor.equals("green")) { // 172-213
                dataslinumber = calc_slicenumber(171, 212, dataratio);
            } else if (data2ndMaxColor.equals("blue"))//214-256
                dataslinumber = calc_slicenumber(213, 255, dataratio);

        } else if (data1stMaxColor.equals("green")) { // cheking slice num 87-171
            if (data2ndMaxColor.equals("red"))//129-171
                dataslinumber = calc_slicenumber(128, 170, dataratio);
            if (data2ndMaxColor.equals("blue"))//87-128
                dataslinumber = calc_slicenumber(86, 127, dataratio);
        } else if (data1stMaxColor.equals("blue")) { // cheking slice num 1-86 = 0-85
            if (data2ndMaxColor.equals("red")) // 1-30
                dataslinumber = calc_slicenumber(0, 29, dataratio);
            if (data2ndMaxColor.equals("green")) // 31-86
                dataslinumber = calc_slicenumber(30, 85, dataratio);
        }

        if (dataslinumber == 0 || maskslinumber == 0) {
            return (int) dataslinumber;
        }


        int gapslicenum = Math.abs(maskslinumber - dataslinumber);
        return gapslicenum;
    }

    private int calc_slicenumber(int lutStartRange, int lutEndRange, double maskratioF) {
        short[][] lut = {
                {127, 0, 255}, {125, 3, 255}, {124, 6, 255}, {122, 9, 255}, {121, 12, 255}, {120, 15, 255}, {119, 18, 255}, {118, 21, 255}, {116, 24, 255}, {115, 27, 255}, {114, 30, 255}, {113, 33, 255},
                {112, 36, 255}, {110, 39, 255}, {109, 42, 255}, {108, 45, 255}, {106, 48, 255}, {105, 51, 255}, {104, 54, 255}, {103, 57, 255}, {101, 60, 255}, {100, 63, 255}, {99, 66, 255}, {98, 69, 255},
                {96, 72, 255}, {95, 75, 255}, {94, 78, 255}, {93, 81, 255}, {92, 84, 255}, {90, 87, 255}, {89, 90, 255}, {87, 93, 255}, {86, 96, 255}, {84, 99, 255}, {83, 102, 255}, {81, 105, 255},
                {80, 108, 255}, {78, 111, 255}, {77, 114, 255}, {75, 117, 255}, {74, 120, 255}, {72, 123, 255}, {71, 126, 255}, {69, 129, 255}, {68, 132, 255}, {66, 135, 255}, {65, 138, 255}, {63, 141, 255},
                {62, 144, 255}, {60, 147, 255}, {59, 150, 255}, {57, 153, 255}, {56, 156, 255}, {54, 159, 255}, {53, 162, 255}, {51, 165, 255}, {50, 168, 255}, {48, 171, 255}, {47, 174, 255}, {45, 177, 255},
                {44, 180, 255}, {42, 183, 255}, {41, 186, 255}, {39, 189, 255}, {38, 192, 255}, {36, 195, 255}, {35, 198, 255}, {33, 201, 255}, {32, 204, 255}, {30, 207, 255}, {29, 210, 255}, {27, 213, 255},
                {26, 216, 255}, {24, 219, 255}, {23, 222, 255}, {21, 225, 255}, {20, 228, 255}, {18, 231, 255}, {16, 234, 255}, {14, 237, 255}, {12, 240, 255}, {9, 243, 255}, {6, 246, 255}, {3, 249, 255},
                {1, 252, 255}, {0, 254, 255}, {3, 255, 252}, {6, 255, 249}, {9, 255, 246}, {12, 255, 243}, {15, 255, 240}, {18, 255, 237}, {21, 255, 234}, {24, 255, 231}, {27, 255, 228}, {30, 255, 225},
                {33, 255, 222}, {36, 255, 219}, {39, 255, 216}, {42, 255, 213}, {45, 255, 210}, {48, 255, 207}, {51, 255, 204}, {54, 255, 201}, {57, 255, 198}, {60, 255, 195}, {63, 255, 192}, {66, 255, 189},
                {69, 255, 186}, {72, 255, 183}, {75, 255, 180}, {78, 255, 177}, {81, 255, 174}, {84, 255, 171}, {87, 255, 168}, {90, 255, 165}, {93, 255, 162}, {96, 255, 159}, {99, 255, 156}, {102, 255, 153},
                {105, 255, 150}, {108, 255, 147}, {111, 255, 144}, {114, 255, 141}, {117, 255, 138}, {120, 255, 135}, {123, 255, 132}, {126, 255, 129}, {129, 255, 126}, {132, 255, 123}, {135, 255, 120},
                {138, 255, 117}, {141, 255, 114}, {144, 255, 111}, {147, 255, 108}, {150, 255, 105}, {153, 255, 102}, {156, 255, 99}, {159, 255, 96}, {162, 255, 93}, {165, 255, 90}, {168, 255, 87}, {171, 255, 84},
                {174, 255, 81}, {177, 255, 78}, {180, 255, 75}, {183, 255, 72}, {186, 255, 69}, {189, 255, 66}, {192, 255, 63}, {195, 255, 60}, {198, 255, 57}, {201, 255, 54}, {204, 255, 51}, {207, 255, 48},
                {210, 255, 45}, {213, 255, 42}, {216, 255, 39}, {219, 255, 36}, {222, 255, 33}, {225, 255, 30}, {228, 255, 27}, {231, 255, 24}, {234, 255, 21}, {237, 255, 18}, {240, 255, 15}, {243, 255, 12},
                {246, 255, 9}, {249, 255, 6}, {252, 255, 3}, {254, 255, 0}, {255, 252, 3}, {255, 249, 6}, {255, 246, 9}, {255, 243, 12}, {255, 240, 15}, {255, 237, 18}, {255, 234, 21}, {255, 231, 24}, {255, 228, 27},
                {255, 225, 30}, {255, 222, 33}, {255, 219, 36}, {255, 216, 39}, {255, 213, 42}, {255, 210, 45}, {255, 207, 48}, {255, 204, 51}, {255, 201, 54}, {255, 198, 57}, {255, 195, 60}, {255, 192, 63},
                {255, 189, 66}, {255, 186, 69}, {255, 183, 72}, {255, 180, 75}, {255, 177, 78}, {255, 174, 81}, {255, 171, 84}, {255, 168, 87}, {255, 165, 90}, {255, 162, 93}, {255, 159, 96}, {255, 156, 99},
                {255, 153, 102}, {255, 150, 105}, {255, 147, 108}, {255, 144, 111}, {255, 141, 114}, {255, 138, 117}, {255, 135, 120}, {255, 132, 123}, {255, 129, 126}, {255, 126, 129}, {255, 123, 132},
                {255, 120, 135}, {255, 117, 138}, {255, 114, 141}, {255, 111, 144}, {255, 108, 147}, {255, 105, 150}, {255, 102, 153}, {255, 99, 156}, {255, 96, 159}, {255, 93, 162}, {255, 90, 165}, {255, 87, 168},
                {255, 84, 171}, {255, 81, 173}, {255, 78, 174}, {255, 75, 175}, {255, 72, 176}, {255, 69, 177}, {255, 66, 178}, {255, 63, 179}, {255, 60, 180}, {255, 57, 181}, {255, 54, 182}, {255, 51, 183},
                {255, 48, 184}, {255, 45, 185}, {255, 42, 186}, {255, 39, 187}, {255, 36, 188}, {255, 33, 189}, {255, 30, 190}, {255, 27, 191}, {255, 24, 192}, {255, 21, 193}, {255, 18, 194}, {255, 15, 195},
                {255, 12, 196}, {255, 9, 197}, {255, 6, 198}, {255, 3, 199}, {255, 0, 200}
        };

        int maskslinumberF = 0;
        double mingapratio = 1000;
        for (int icolor = lutStartRange; icolor <= lutEndRange; icolor++) {

            short[] coloraray = lut[icolor];
            double lutRatio = 0;

            double colorR = coloraray[0];
            double colorG = coloraray[1];
            double colorB = coloraray[2];

            if (colorB > colorR && colorB > colorG) {
                if (colorR > colorG)
                    lutRatio = colorR / colorB;
                else if (colorG > colorR)
                    lutRatio = colorG / colorB;
            } else if (colorG > colorR && colorG > colorB) {
                if (colorR > colorB)
                    lutRatio = colorR / colorG;
                else if (colorB > colorR)
                    lutRatio = colorB / colorG;
            } else if (colorR > colorG && colorR > colorB) {
                if (colorG > colorB)
                    lutRatio = colorG / colorR;
                else if (colorB > colorG)
                    lutRatio = colorB / colorR;
            }

            if (lutRatio == maskratioF) {
                maskslinumberF = icolor + 1;
                break;
            }

            double gapratio = Math.abs(maskratioF - lutRatio);

            if (gapratio < mingapratio) {
                mingapratio = gapratio;
                maskslinumberF = icolor + 1;
            }
        }
        return maskslinumberF;
    }

    private void startAndJoin(Thread[] threads) {
        for (Thread thread : threads) {
            thread.setPriority(Thread.NORM_PRIORITY);
            thread.start();
        }

        try {
            for (int ithread = 0; ithread < threads.length; ++ithread)
                threads[ithread].join();
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
    }

}
