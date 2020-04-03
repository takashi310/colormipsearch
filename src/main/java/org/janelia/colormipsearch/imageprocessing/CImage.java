package org.janelia.colormipsearch.imageprocessing;

import java.util.function.Function;

class CImage {

    int[] imageCache;
    ImageTransformation.ColorHistogram histogram;
    LImage lImage;
    int x;
    int y;

    CImage(LImage lImage, int x, int y) {
        this.lImage = lImage;
        this.x = x;
        this.y = y;
    }

    int extract() {
        return lImage.get(x, y);
    }

    CImage extend(Function<CImage, Integer> f) {
        CImage arg = new CImage(lImage, 0, 0);
        return new CImage(lImage.mapi((i, j) -> {
            arg.x = i;
            arg.y = j;
            return f.apply(arg);
        }), x, y);
    }

    CImage fmap(ColorTransformation f) {
        return new CImage(lImage.map(f), x, y);
    }

    int get(int i, int j) {
        return lImage.get(x + i, y + j);
    }

    ImageArray toImageArray() {
        return lImage.asImageArray();
    }

}
