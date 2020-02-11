package org.janelia.colormipsearch;

import java.util.Random;

/**
 * This is some temporary test data that will be thrown away once we have correct published URLs.
 */
class TestData {
    static Random random = new Random();

    static class ImageWithThumnailURL {
        final String fullImageURL;
        final String thumbnailImageURL;

        ImageWithThumnailURL(String fullImageURL, String thumbnailImageURL) {
            this.fullImageURL = fullImageURL;
            this.thumbnailImageURL = thumbnailImageURL;
        }
    }
    static String[] TEST_URLS = new String[] {
            "https://%s.s3.amazonaws.com/JRC2018_Unisex_20x_HR/FlyLight+Split-GAL4+Drivers/LH1989-20160902_22_A3-f-20x-brain-JRC2018_Unisex_20x_HR-color_depth_1.png",
            "https://%s.s3.amazonaws.com/JRC2018_Unisex_20x_HR/FlyLight+Split-GAL4+Drivers/LH1000-20151106_52_J3-f-20x-brain-JRC2018_Unisex_20x_HR-color_depth_1.png",
            "https://%s.s3.amazonaws.com/JRC2018_Unisex_20x_HR/FlyLight+Split-GAL4+Drivers/LH1046-20151202_33_I1-f-20x-brain-JRC2018_Unisex_20x_HR-color_depth_1.png"
    };

    static ImageWithThumnailURL aRandomURL() {
        String urlPattern = TEST_URLS[random.nextInt(TEST_URLS.length)];
        return new ImageWithThumnailURL(
                String.format(urlPattern, "/janelia-flylight-color-depth"),
                String.format(urlPattern, "janelia-flylight-color-depth-thumbnails"));
    }
}
