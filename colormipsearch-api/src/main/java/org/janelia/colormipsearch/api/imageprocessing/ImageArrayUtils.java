package org.janelia.colormipsearch.api.imageprocessing;

import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.Arrays;

import javax.imageio.ImageIO;

import ij.ImagePlus;
import ij.io.FileInfo;
import ij.io.Opener;
import ij.io.RandomAccessStream;
import ij.io.TiffDecoder;
import ij.process.ByteProcessor;
import ij.process.ColorProcessor;
import ij.process.ImageProcessor;
import ij.process.ShortProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Image Utils.
 */
public class ImageArrayUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ImageArrayUtils.class);

    private enum ImageFormat {
        PNG,
        TIFF,
        UNKNOWN
    }

    /**
     * Read an image array from an ImageJ ImagePlus object.
     * @param imagePlus
     * @return
     */
    public static ImageArray fromImagePlus(ImagePlus imagePlus) {
        ImageType type = ImageType.fromImagePlusType(imagePlus.getType());
        ImageProcessor ip = imagePlus.getProcessor();
        int width = ip.getWidth();
        int height = ip.getHeight();

        ImageArray ret = null;

        switch (type) {
            case GRAY8:
                ret = new ByteImageArray(type, width, height, (byte[])ip.getPixels());
                break;
            case GRAY16:
                ret = new ShortImageArray(type, width, height, (short[])ip.getPixels());
                break;
            case RGB:
                ret = new ColorImageArray(type, width, height, (int[])ip.getPixels());
                break;
        }

        return ret;
    }

    /**
     * Determine if the file identified by the given name is an image file. This is only based on the filename extension.
     * @param name - file name
     * @return
     */
    public static boolean isImageFile(String name) {
        int extseparator = name.lastIndexOf('.');
        if (extseparator == -1) {
            return false;
        }
        String fext = name.substring(extseparator + 1);
        switch (fext.toLowerCase()) {
            case "jpg":
            case "jpeg":
            case "png":
            case "tif":
            case "tiff":
                return true;
            default:
                return false;
        }
    }

    /**
     * Read an image array from a byte stream.
     *
     * @param title image title
     * @param name image (file) name used only for determining the image encoding
     * @param stream image pixels stream
     * @return
     * @throws Exception
     */
    public static ImageArray readImageArray(String title, String name, InputStream stream) throws Exception {
        ImageFormat format = getImageFormat(name);
        ImagePlus imagePlus;
        switch (format) {
            case PNG:
                imagePlus = readPngToImagePlus(title, stream);
                break;
            case TIFF:
                imagePlus = readTiffToImagePlus(title, stream);
                break;
            default:
                throw new IllegalArgumentException("Image '" + name + "' must be in PNG or TIFF format");
        }
        try {
            return fromImagePlus(imagePlus);
        } finally {
            imagePlus.close();
        }
    }

    private static int packBitsUncompress(byte[] input, byte[] output, int offset, int bound) {
        if (bound==0) bound = Integer.MAX_VALUE;
        int index = 0;
        int pos = offset;
        while (pos < bound && pos < output.length && index < input.length) {
            byte n = input[index++];
            if (n>=0) { // 0 <= n <= 127
                byte[] b = new byte[n+1];
                for (int i=0; i<n+1; i++)
                    b[i] = input[index++];
                System.arraycopy(b, 0, output, pos, b.length);
                pos += (int)b.length;
                b = null;
            } else if (n != -128) { // -127 <= n <= -1
                int len = -n + 1;
                byte inp = input[index++];
                for (int i=0; i<len; i++) output[pos++] = inp;
            }
        }
        return pos;
    }

    public static ImageArray readImageArrayRange(String title, String name, InputStream stream, long start, long end) throws Exception {
        ImageFormat format = getImageFormat(name);
        byte[] img_bytearr = null;
        ImagePlus imagePlus = null;

        int maskpos_st = (int)start * 3;
        int maskpos_ed = (int)end * 3;
        int stripsize = maskpos_ed - maskpos_st + 3;
        int width = 0;
        int height = 0;

        TiffDecoderJ tfd = null;
        RandomAccessStream ras = null;

        ImageArray ret = null;

        try {
            //pixels = new int[x * y];
            switch (format) {
                case PNG:
                    imagePlus = readPngToImagePlus(title, stream);
                    ret = fromImagePlus(imagePlus);
                    break;
                case TIFF:
                    //imagePlus = readTiffToImagePlus(title, stream);
                    tfd = new TiffDecoderJ(stream, title);

                    if (tfd != null)
                    {
                        FileInfo[] fi_list = tfd.getTiffInfo();
                        if (fi_list != null && fi_list[0] != null)
                        {
                            ras = tfd.getRandomAccessStream();
                            width = fi_list[0].width;
                            height = fi_list[0].height;
                            int dsize = width * height * 3;

                            long loffset = 0;
                            int stripid = 0;
                            if (fi_list[0].compression != 5) {
                                //byte[] byteArray = new byte[stripsize];
                                img_bytearr = new byte[dsize];
                                loffset = fi_list[0].getOffset();
                                ras.seek(loffset + (long)maskpos_st);
                                ras.read(img_bytearr, maskpos_st, stripsize);
                            } else {
                                int ioffset = 0;
                                img_bytearr = new byte[dsize];
                                for (int i = 0; i < fi_list[0].stripOffsets.length; i++) {
                                    ras.seek(fi_list[0].stripOffsets[i]);
                                    byte[] byteArray = new byte[fi_list[0].stripLengths[i]];
                                    int read = 0, left = byteArray.length;
                                    while (left > 0) {
                                        int r = ras.read(byteArray, read, left);
                                        if (r == -1) break;
                                        read += r;
                                        left -= r;
                                    }
                                    loffset = ioffset;
                                    stripid = i;
                                    ioffset = packBitsUncompress(byteArray, img_bytearr, ioffset, maskpos_ed);
                                    if (ioffset >= maskpos_ed) {
                                        break;
                                    }
                                }
                            }
                            ret = new ColorImageArray(ImageType.fromImagePlusType(ImagePlus.COLOR_RGB), width, height, img_bytearr);
                        }
                    }

                    break;
                default:
                    throw new IllegalArgumentException("Image '" + name + "' must be in PNG or TIFF format");
            }
        } finally {
            if (ras != null)
                ras.close();
            if (imagePlus != null)
                imagePlus.close();
        }

        return ret;
    }

    private static ImageFormat getImageFormat(String name) {
        String lowerCaseName = name.toLowerCase();

        if (lowerCaseName.endsWith(".png")) {
            return ImageFormat.PNG;
        } else if (lowerCaseName.endsWith(".tiff") || lowerCaseName.endsWith(".tif")) {
            return ImageFormat.TIFF;
        }

        LOG.warn("Unrecognized format from {} - so far it only supports PNG and TIFF", name);
        return ImageFormat.UNKNOWN;
    }

    private static ImagePlus readPngToImagePlus(String title, InputStream stream) throws Exception {
        return new ImagePlus(title, ImageIO.read(stream));
    }

    private static ImagePlus readTiffToImagePlus(String title, InputStream stream) throws Exception {
        return new Opener().openTiff(stream, title);
    }

    public static ImageProcessor toImageProcessor(ImageArray imageArray) {
        switch (imageArray.type) {
            case GRAY8:
                return new ByteProcessor(imageArray.width, imageArray.height, Arrays.copyOf((byte[])imageArray.getPixels(), ((byte[])imageArray.getPixels()).length));
            case GRAY16:
                return new ShortProcessor(imageArray.width, imageArray.height, Arrays.copyOf((short[])imageArray.getPixels(), ((short[])imageArray.getPixels()).length), null /* default color model */);
            default:
                int[] intImageBuffer = new int[imageArray.width * imageArray.height];
                for (int i = 0; i < intImageBuffer.length; i++) {
                    intImageBuffer[i] = imageArray.get(i);
                }
                return new ColorProcessor(imageArray.width, imageArray.height, Arrays.copyOf(intImageBuffer, intImageBuffer.length));
        }
    }

}
