package org.janelia.colormipsearch;

public class ScoreAdjuster {

    private void maskBodyIdLabel(MIPWithPixels mip) {
        int pix0 = mip.getPixel(0, 0);
        for (int ix = 0; ix < 250 && ix < mip.width; ix++) {
            for (int iy = 0; iy < 40 && iy < mip.height; iy++) {

                int pixf = mip.getPixel(ix, iy);

                int red1 = (pixf >>> 16) & 0xff;
                int green1 = (pixf >>> 8) & 0xff;
                int blue1 = pixf & 0xff;

                if (red1 > 0 && green1 > 0 && blue1 > 0)
                    mip.setPixel(ix, iy, pix0);
            }
        }
    }

}
