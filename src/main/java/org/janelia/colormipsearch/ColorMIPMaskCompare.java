package org.janelia.colormipsearch;

import ij.ImagePlus;
import ij.process.ImageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Hideo Otsuna.
 */
public class ColorMIPMaskCompare
{
	private static final Logger log = LoggerFactory.getLogger(ColorMIPMaskCompare.class);

	public static class Parameters {
        /** The image to search within */
        ImagePlus searchImage;
        /** The mask image */
        ImagePlus maskImage;
		/** Threshold for mask */
        int Thresm = 50;
        /** Threshold for data */
        int Thres = 100;
        /** % of Positive PX Threshold 0-100% */
        double pixThres = 2;
        /** Pix Color Fluctuation, 1.18 per slice */
        double pixflu = 2;
    }

    public static class Output {
        double matchingSlicesPct;
        int matchingSlices;
        boolean isMatch;
    }

	public Output runSearch(Parameters params) {

        ImageProcessor ip1, ip3;
        int pix1 = 0;

        int Thres = params.Thres;
        double pixThres = params.pixThres;
        double pixflu = params.pixflu;
        int Thresm = params.Thresm;

        double pixfludub = pixflu / 100;

        double pixThresdub = pixThres / 100;///10000
        ImagePlus imask = params.maskImage;
        ImagePlus idata = params.searchImage;

		ip1 = imask.getProcessor(); //Mask

        if (ip1.getBitDepth()!=24) {
            throw new IllegalArgumentException("Mask image must have 24 bit color depth");
        }

		int sumpx = ip1.getPixelCount();

        int red1 = 0;
        int green1 = 0;
        int blue1 = 0;

        int red2 = 0;
        int green2 = 0;
        int blue2 = 0;

        int posislice = 0;
        int posi = 0;

        int masksize=0;

		int [] maskposi = new int [sumpx];

		///Mask size measurement///////////////////////////////
		for(int n4=0; n4<sumpx; n4++){

			pix1= ip1.get(n4);//Mask

			red1 = (pix1>>>16) & 0xff;//mask
			green1 = (pix1>>>8) & 0xff;//mask
			blue1 = pix1 & 0xff;//mask

			if(red1>Thresm || green1>Thresm || blue1>Thresm){//Mask value
				maskposi[masksize]=n4;
				masksize=masksize+1;
			}
		}

        ip3 = idata.getProcessor();
        if (ip3.getBitDepth()!=24) {
            throw new IllegalArgumentException("Mask image must have 24 bit color depth");
        }

        posi=0;
        for(int masksig=0; masksig<masksize; masksig++){

            pix1= ip1.get(maskposi[masksig]);//Mask, array

            red1 = (pix1>>>16) & 0xff;//mask
            green1 = (pix1>>>8) & 0xff;//mask
            blue1 = pix1 & 0xff;//mask

            int pix2= ip3.get(maskposi[masksig]);// data
            red2 = (pix2>>>16) & 0xff;//data
            green2 = (pix2>>>8) & 0xff;//data
            blue2 = pix2 & 0xff;//data

            if(red2>Thres || green2>Thres || blue2>Thres){

                int RG1=0; int BG1=0; int GR1=0; int GB1=0; int RB1=0; int BR1=0;
                int RG2=0; int BG2=0; int GR2=0; int GB2=0; int RB2=0; int BR2=0;
                double rb1=0; double rg1=0; double gb1=0; double gr1=0; double br1=0; double bg1=0;
                double rb2=0; double rg2=0; double gb2=0; double gr2=0; double br2=0; double bg2=0;
                double pxGap=10000;
                double BrBg=0.354862745; double BgGb=0.996078431; double GbGr=0.505882353; double GrRg=0.996078431; double RgRb=0.505882353;
                double BrGap=0; double BgGap=0; double GbGap=0; double GrGap=0; double RgGap=0; double RbGap=0;

                String checkborder="";

                if(blue1>red1 && blue1>green1){//1,2
                    if(red1>green1){
                        BR1=blue1+red1;//1
                        if(blue1!=0 && red1!=0)
                        br1= (double) red1 / (double) blue1;
                    }else{
                        BG1=blue1+green1;//2
                        if(blue1!=0 && green1!=0)
                        bg1= (double) green1 / (double) blue1;
                    }
                }else if(green1>blue1 && green1>red1){//3,4
                    if(blue1>red1){
                        GB1=green1+blue1;//3
                        if(green1!=0 && blue1!=0)
                        gb1= (double) blue1 / (double) green1;
                    }else{
                        GR1=green1+red1;//4
                        if(green1!=0 && red1!=0)
                        gr1= (double) red1 / (double) green1;
                    }
                }else if(red1>blue1 && red1>green1){//5,6
                    if(green1>blue1){
                        RG1=red1+green1;//5
                        if(red1!=0 && green1!=0)
                        rg1= (double) green1 / (double) red1;
                    }else{
                        RB1=red1+blue1;//6
                        if(red1!=0 && blue1!=0)
                        rb1= (double) blue1 / (double) red1;
                    }
                }

                if(blue2>red2 && blue2>green2){
                    if(red2>green2){//1, data
                        BR2=blue2+red2;
                        if(blue2!=0 && red2!=0)
                        br2= (double) red2 / (double) blue2;
                    }else{//2, data
                        BG2=blue2+green2;
                        if(blue2!=0 && green2!=0)
                        bg2= (double) green2 / (double) blue2;
                    }
                }else if(green2>blue2 && green2>red2){
                    if(blue2>red2){//3, data
                        GB2=green2+blue2;
                        if(green2!=0 && blue2!=0)
                        gb2= (double) blue2 / (double) green2;
                    }else{//4, data
                        GR2=green2+red2;
                        if(green2!=0 && red2!=0)
                        gr2= (double) red2 / (double) green2;
                    }
                }else if(red2>blue2 && red2>green2){
                    if(green2>blue2){//5, data
                        RG2=red2+green2;
                        if(red2!=0 && green2!=0)
                        rg2= (double) green2 / (double) red2;
                    }else{//6, data
                        RB2=red2+blue2;
                        if(red2!=0 && blue2!=0)
                        rb2= (double) blue2 / (double) red2;
                    }
                }

                ///////////////////////////////////////////////////////
                if(BR1>0){//1, mask// 2 color advance core
                    if(BR2>0){//1, data
                        if(br1>0 && br2>0){
                            if(br1!=br2){
                                pxGap=br2-br1;
                                pxGap=Math.abs(pxGap);
                            }else
                            pxGap=0;

                            if(br1==255 & br2==255)
                            pxGap=1000;
                        }
                    }else if (BG2>0){//2, data
                        if(br1<0.44 && bg2<0.54){
                            BrGap=br1-BrBg;//BrBg=0.354862745;
                            BgGap=bg2-BrBg;//BrBg=0.354862745;
                            pxGap=BrGap+BgGap;
                        }
                    }
                    //		log.info("pxGap; "+String.valueOf(pxGap)+"  BR1;"+String.valueOf(BR1)+", br1; "+String.valueOf(br1)+", BR2; "+String.valueOf(BR2)+", br2; "+String.valueOf(br2)+", BG2; "+String.valueOf(BG2)+", bg2; "+String.valueOf(bg2));
                }else if(BG1>0){//2, mask/////////////////////////////
                    if(BG2>0){//2, data, 2,mask

                        if(bg1>0 && bg2>0){
                            if(bg1!=bg2){
                                pxGap=bg2-bg1;
                                pxGap=Math.abs(pxGap);

                            }else if(bg1==bg2)
                            pxGap=0;
                            if(bg1==255 & bg2==255)
                            pxGap=1000;
                        }
                        //	log.info(" pxGap BG2;"+String.valueOf(pxGap)+", bg1; "+String.valueOf(bg1)+", bg2; "+String.valueOf(bg2));
                    }else if(GB2>0){//3, data, 2,mask
                        if(bg1>0.8 && gb2>0.8){
                            BgGap=BgGb-bg1;//BgGb=0.996078431;
                            GbGap=BgGb-gb2;//BgGb=0.996078431;
                            pxGap=BgGap+GbGap;
                            //			log.info(" pxGap GB2;"+String.valueOf(pxGap));
                        }
                    }else if(BR2>0){//1, data, 2,mask
                        if(bg1<0.54 && br2<0.44){
                            BgGap=bg1-BrBg;//BrBg=0.354862745;
                            BrGap=br2-BrBg;//BrBg=0.354862745;
                            pxGap=BrGap+BgGap;
                        }
                    }
                    //		log.info("pxGap; "+String.valueOf(pxGap)+"  BG1;"+String.valueOf(BG1)+"  BG2;"+String.valueOf(BG2)+", bg1; "+String.valueOf(bg1)+", bg2; "+String.valueOf(bg2)+", GB2; "+String.valueOf(GB2)+", gb2; "+String.valueOf(gb2)+", BR2; "+String.valueOf(BR2)+", br2; "+String.valueOf(br2));
                }else if(GB1>0){//3, mask/////////////////////////////
                    if(GB2>0){//3, data, 3mask
                        if(gb1>0 && gb2>0){
                            if(gb1!=gb2){
                                pxGap=gb2-gb1;
                                pxGap=Math.abs(pxGap);

                                //	log.info(" pxGap GB2;"+String.valueOf(pxGap));
                            }else
                            pxGap=0;
                            if(gb1==255 & gb2==255)
                            pxGap=1000;
                        }
                    }else if(BG2>0){//2, data, 3mask
                        if(gb1>0.8 && bg2>0.8){
                            BgGap=BgGb-gb1;//BgGb=0.996078431;
                            GbGap=BgGb-bg2;//BgGb=0.996078431;
                            pxGap=BgGap+GbGap;
                        }
                    }else if(GR2>0){//4, data, 3mask
                        if(gb1<0.7 && gr2<0.7){
                            GbGap=gb1-GbGr;//GbGr=0.505882353;
                            GrGap=gr2-GbGr;//GbGr=0.505882353;
                            pxGap=GbGap+GrGap;
                        }
                    }//2,3,4 data, 3mask
                }else if(GR1>0){//4mask/////////////////////////////
                    if(GR2>0){//4, data, 4mask
                        if(gr1>0 && gr2>0){
                            if(gr1!=gr2){
                                pxGap=gr2-gr1;
                                pxGap=Math.abs(pxGap);
                            }else
                            pxGap=0;
                            if(gr1==255 & gr2==255)
                            pxGap=1000;
                        }
                    }else if(GB2>0){//3, data, 4mask
                        if(gr1<0.7 && gb2<0.7){
                            GrGap=gr1-GbGr;//GbGr=0.505882353;
                            GbGap=gb2-GbGr;//GbGr=0.505882353;
                            pxGap=GrGap+GbGap;
                        }
                    }else if(RG2>0){//5, data, 4mask
                        if(gr1>0.8 && rg2>0.8){
                            GrGap=GrRg-gr1;//GrRg=0.996078431;
                            RgGap=GrRg-rg2;
                            pxGap=GrGap+RgGap;
                        }
                    }//3,4,5 data
                }else if(RG1>0){//5, mask/////////////////////////////
                    if(RG2>0){//5, data, 5mask
                        if(rg1>0 && rg2>0){
                            if(rg1!=rg2){
                                pxGap=rg2-rg1;
                                pxGap=Math.abs(pxGap);
                            }else
                            pxGap=0;
                            if(rg1==255 & rg2==255)
                            pxGap=1000;
                        }

                    }else if(GR2>0){//4 data, 5mask
                        if(rg1>0.8 && gr2>0.8){
                            GrGap=GrRg-gr2;//GrRg=0.996078431;
                            RgGap=GrRg-rg1;//GrRg=0.996078431;
                            pxGap=GrGap+RgGap;
                            //	log.info(" pxGap GR2;"+String.valueOf(pxGap));
                        }
                    }else if(RB2>0){//6 data, 5mask
                        if(rg1<0.7 && rb2<0.7){
                            RgGap=rg1-RgRb;//RgRb=0.505882353;
                            RbGap=rb2-RgRb;//RgRb=0.505882353;
                            pxGap=RbGap+RgGap;
                        }
                    }//4,5,6 data
                }else if(RB1>0){//6, mask/////////////////////////////
                    if(RB2>0){//6, data, 6mask
                        if(rb1>0 && rb2>0){
                            if(rb1!=rb2){
                                pxGap=rb2-rb1;
                                pxGap=Math.abs(pxGap);
                            }else if(rb1==rb2)
                            pxGap=0;
                            if(rb1==255 & rb2==255)
                            pxGap=1000;
                        }
                    }else if(RG2>0){//5, data, 6mask
                        if(rg2<0.7 && rb1<0.7){
                            RgGap=rg2-RgRb;//RgRb=0.505882353;
                            RbGap=rb1-RgRb;//RgRb=0.505882353;
                            pxGap=RgGap+RbGap;
                            //	log.info(" pxGap RG;"+String.valueOf(pxGap));
                        }
                    }
                }//2 color advance core

//                log.info("" + pxGap + "<=" + pixfludub + "?");
                if(pxGap<=pixfludub){
                    posi=posi+1;
                }
                else if(pxGap==1000) {
                    log.info("There is 255 x2 value");
                }

            }//if(red2>Thres || green2>Thres || blue2>Thres){
        }//for(int masksig=0; masksig<masksize; masksig++){

        double posipersent= (double) posi/ (double) masksize;

        Output output = new Output();
        output.matchingSlicesPct = posipersent;
        output.matchingSlices = posi;
        output.isMatch = posipersent>pixThresdub;

//        log.info("Matching slices: "+posi);
        if(posipersent>pixThresdub){
            posislice=posislice+1;
        }//if(posipersent>pixThresdub){

		imask.unlock();
		idata.unlock();

		//	log.info("Done; "+increment+" mean; "+mean3+" Totalmaxvalue; "+totalmax+" desiremean; "+desiremean);

        return output;
	} //public Output runSearch(Parameters params) {
} //public class ColorMIPMaskCompare {



























