package org.janelia.colormipsearch.api_v2.bdssearch;
import net.imglib2.Cursor;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.IntegerType;
import net.imglib2.type.numeric.integer.UnsignedByteType;

public class ImageANDOperation {

    public static < T extends IntegerType<T> > Img<T> andOperation(Img<T> img1, Img<T> img2) {
        // Assuming img1 and img2 are of the same dimensions
        Img<T> resultImg = img1.factory().create(img1, img1.firstElement());

        Cursor<T> cursor1 = img1.cursor();
        Cursor<T> cursor2 = img2.cursor();
        Cursor<T> resultCursor = resultImg.cursor();

        while (cursor1.hasNext() && cursor2.hasNext()) {
            cursor1.fwd();
            cursor2.fwd();
            resultCursor.fwd();

            // Perform the AND operation on the pixel values
            int andResult = cursor1.get().getInteger() & cursor2.get().getInteger();
            resultCursor.get().setInteger(andResult);
        }

        return resultImg;
    }
}
