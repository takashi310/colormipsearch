package org.janelia.colormipsearch.api.imageprocessing;

import java.io.InputStream;
import java.util.Arrays;

import javax.imageio.ImageIO;

import ij.ImagePlus;
import ij.io.Opener;
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
        BMP,
        GIF,
        JPG,
        PNG,
        TIFF,
        WBMP,
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
        int[] pixels = new int[width * height];
        for (int pi = 0; pi < width * height; pi++) {
            pixels[pi] = ip.get(pi);
        }
        return new ImageArray(type, width, height, pixels);
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
            case "bmp":
            case "gif":
            case "jpg":
            case "jpeg":
            case "png":
            case "tif":
            case "tiff":
            case "wbmp":
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
            case BMP:
            case GIF:
            case JPG:
            case PNG:
            case WBMP:
                imagePlus = readImagePlusWithImageIO(title, stream);
                break;
            case TIFF:
                imagePlus = readImagePlusWithTiffReader(title, stream);
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

    private static ImageFormat getImageFormat(String name) {
        String lowerCaseName = name.toLowerCase();

        if (lowerCaseName.endsWith(".bmp")) {
            return ImageFormat.BMP;
        } else if (lowerCaseName.endsWith(".gif")) {
            return ImageFormat.GIF;
        } else if (lowerCaseName.endsWith(".jpg") || lowerCaseName.endsWith(".jpeg")) {
            return ImageFormat.JPG;
        } else if (lowerCaseName.endsWith(".png")) {
            return ImageFormat.PNG;
        } else if (lowerCaseName.endsWith(".tiff") || lowerCaseName.endsWith(".tif")) {
            return ImageFormat.TIFF;
        } else if (lowerCaseName.endsWith(".wbmp")) {
            return ImageFormat.WBMP;
        }

        LOG.warn("Unrecognized format from {} - so far it only supports BMP, GIF, JPG, PNG, TIFF, and WBMP", name);
        return ImageFormat.UNKNOWN;
    }

    private static ImagePlus readImagePlusWithImageIO(String title, InputStream stream) throws Exception {
        return new ImagePlus(title, ImageIO.read(stream));
    }

    private static ImagePlus readImagePlusWithTiffReader(String title, InputStream stream) throws Exception {
        return new Opener().openTiff(stream, title);
    }

    public static ImageProcessor toImageProcessor(ImageArray imageArray) {
        switch (imageArray.type) {
            case GRAY8:
                byte[] byteImageBuffer = new byte[imageArray.pixels.length];
                for (int i = 0; i < imageArray.pixels.length; i++) {
                    byteImageBuffer[i] = (byte) (imageArray.pixels[i] & 0xFF);
                }
                return new ByteProcessor(imageArray.width, imageArray.height, byteImageBuffer);
            case GRAY16:
                short[] shortImageBuffer = new short[imageArray.pixels.length];
                for (int i = 0; i < imageArray.pixels.length; i++) {
                    shortImageBuffer[i] = (short) (imageArray.pixels[i] & 0xFFFF);
                }
                return new ShortProcessor(imageArray.width, imageArray.height, shortImageBuffer, null /* default color model */);
            default:
                return new ColorProcessor(imageArray.width, imageArray.height, Arrays.copyOf(imageArray.pixels, imageArray.pixels.length));
        }
    }

}
