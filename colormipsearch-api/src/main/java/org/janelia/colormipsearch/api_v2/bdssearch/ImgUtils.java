package org.janelia.colormipsearch.api_v2.bdssearch;

import ij.ImagePlus;
import ij.ImageStack;
import ij.process.ImageProcessor;
import io.scif.SCIFIO;
import io.scif.config.SCIFIOConfig;
import io.scif.img.ImgSaver;
import loci.formats.FormatException;
import loci.formats.IFormatReader;
import loci.formats.ImageReader;
import net.imglib2.Cursor;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.Type;
import net.imglib2.type.numeric.ARGBType;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import org.janelia.colormipsearch.api_v2.cdmips.MIPMetadata;
import org.janelia.colormipsearch.imageprocessing.*;
import org.janelia.colormipsearch.mips.NeuronMIPUtils;
import org.janelia.colormipsearch.model.FileData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.janelia.colormipsearch.imageprocessing.ImageArrayUtils.*;

public class ImgUtils {

    public static Img<?> readNRRD(String filePath) throws IOException, FormatException {
        IFormatReader reader = new ImageReader();
        reader.setId(filePath);

        int bitsPerPixel = reader.getBitsPerPixel();

        Img<?> img;
        if (bitsPerPixel <= 8) {
            img = readImageToImg(reader, new UnsignedByteType());
        } else if (bitsPerPixel <= 16) {
            img = readImageToImg(reader, new UnsignedShortType());
        } else if (bitsPerPixel <= 32) {
            img = readImageToImg(reader, new FloatType());
        } else {
            throw new IllegalArgumentException("Unsupported bit depth: " + bitsPerPixel);
        }

        return img;
    }

    private static <T extends NativeType<T>> Img<T> readImageToImg(IFormatReader reader, T type) throws FormatException, IOException {
        int width = reader.getSizeX();
        int height = reader.getSizeY();
        int depth = reader.getSizeZ();
        int numPixels = width * height;

        // Create an Img object with the appropriate type and dimensions
        ArrayImgFactory<T> imgFactory = new ArrayImgFactory<>(type);
        Img<T> img = imgFactory.create(new long[]{width, height, depth});

        int bytesPerPixel = reader.getBitsPerPixel() / 8;
        byte[] imgData = new byte[numPixels * bytesPerPixel];

        if (type instanceof UnsignedByteType) {
            Cursor<UnsignedByteType> cursor = (Cursor<UnsignedByteType>) img.cursor();
            for (int z = 0; z < depth; z++) {
                reader.openBytes(z, imgData);
                for (int p = 0; p < numPixels; p++) {
                    cursor.next().setInteger(imgData[p] & 0xff);
                }
            }
        } else if (type instanceof UnsignedShortType) {
            Cursor<UnsignedShortType> cursor = (Cursor<UnsignedShortType>) img.cursor();
            for (int z = 0; z < depth; z++) {
                reader.openBytes(z, imgData);
                ByteBuffer buffer = ByteBuffer.wrap(imgData).order(ByteOrder.BIG_ENDIAN);
                for (int p = 0; p < numPixels; p++) {
                    cursor.next().setInteger(buffer.getShort() & 0xffff);
                }
            }
        } else if (type instanceof FloatType) {
            Cursor<FloatType> cursor = (Cursor<FloatType>) img.cursor();
            for (int z = 0; z < depth; z++) {
                reader.openBytes(z, imgData);
                ByteBuffer buffer = ByteBuffer.wrap(imgData).order(ByteOrder.BIG_ENDIAN);
                for (int p = 0; p < numPixels; p++) {
                    cursor.next().setReal(buffer.getFloat());
                }
            }
        }

        return img;
    }

    public static void saveImgAsPng(Img<ARGBType> img, String filePath) {
        try {
            BufferedImage bufferedImage = convertToBufferedImage(img);
            ImageIO.write(bufferedImage, "PNG", new File(filePath));
        } catch (Exception e) {
            e.printStackTrace();
        }
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
                int argb = 0xFF000000 | ( ((array[3*i] & 0xFF) << 16) | ((array[3*i+1] & 0xFF) << 8) | (array[3*i+2] & 0xFF) );
                cursor.next().set(argb);
                i++;
            }
            return imp;
        } else {
            throw new IllegalArgumentException("Unsupported image type");
        }
    }

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

    public static BufferedImage convertToBufferedImage(Img<ARGBType> img) {
        // Assuming the image is non-empty, get its dimensions
        int width = (int) img.max(0) + 1;
        int height = (int) img.max(1) + 1;

        // Create a BufferedImage of type TYPE_INT_ARGB
        BufferedImage bufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);

        // Use a cursor to iterate over the Img<ARGBType>
        Cursor<ARGBType> cursor = img.cursor();

        while (cursor.hasNext()) {
            cursor.fwd();
            int x = cursor.getIntPosition(0);
            int y = cursor.getIntPosition(1);
            int argb = cursor.get().get();

            // Set the ARGB value in the BufferedImage
            bufferedImage.setRGB(x, y, argb);
        }

        return bufferedImage;
    }

    public static void saveImageAsPNG(Img<ARGBType> image, String outputPath) {
        // Convert Img<ARGBType> to BufferedImage
        BufferedImage buffered_image = convertToBufferedImage(image);
        // Use ImageIO to write the BufferedImage to a file
        try {
            File outputFile = new File(outputPath);
            ImageIO.write(buffered_image, "PNG", outputFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static short argbToGray16(int argb) {
        if ((argb & 0x00FFFFFF) == 0) {
            return 0;
        } else {
            int r = (argb >> 16) & 0xFF;
            int g = (argb >> 8) & 0xFF;
            int b = (argb & 0xFF);

            double rw = 1 / 3.;
            double gw = 1 / 3.;
            double bw = 1 / 3.;

            return (short) (r*rw + g*gw + b*bw + 0.5);
        }
    }

    public static Img<UnsignedShortType> ConvertARGBToGray16(Img<ARGBType> input) {
        long w = input.dimension(0);
        long h = input.dimension(1);

        ArrayImgFactory<UnsignedShortType> factory = new ArrayImgFactory<>(new UnsignedShortType());
        Img<UnsignedShortType> result = factory.create(w, h);

        Cursor<ARGBType> srcCursor = input.cursor();
        Cursor<UnsignedShortType> dstCursor = result.cursor();
        while (srcCursor.hasNext() && dstCursor.hasNext()) {
            dstCursor.next().set(argbToGray16(srcCursor.next().get()));
        }
        return result;
    }

    public static Img<FloatType> ConvertARGBToFloat32(Img<ARGBType> input) {
        long w = input.dimension(0);
        long h = input.dimension(1);

        ArrayImgFactory<FloatType> factory = new ArrayImgFactory<>(new FloatType());
        Img<FloatType> result = factory.create(w, h);

        Cursor<ARGBType> srcCursor = input.cursor();
        Cursor<FloatType> dstCursor = result.cursor();
        while (srcCursor.hasNext() && dstCursor.hasNext()) {
            dstCursor.next().set((float)argbToGray16(srcCursor.next().get()));
        }
        return result;
    }

    public static Img<FloatType> ConvertGray16ToFloat32(Img<UnsignedShortType> input) {
        long w = input.dimension(0);
        long h = input.dimension(1);

        ArrayImgFactory<FloatType> factory = new ArrayImgFactory<>(new FloatType());
        Img<FloatType> result = factory.create(w, h);

        Cursor<UnsignedShortType> srcCursor = input.cursor();
        Cursor<FloatType> dstCursor = result.cursor();
        while (srcCursor.hasNext() && dstCursor.hasNext()) {
            dstCursor.next().set((float)srcCursor.next().get());
        }
        return result;
    }

    public static Img<UnsignedShortType> ConvertFloat32ToGray16(Img<FloatType> input) {
        long w = input.dimension(0);
        long h = input.dimension(1);

        ArrayImgFactory<UnsignedShortType> factory = new ArrayImgFactory<>(new UnsignedShortType());
        Img<UnsignedShortType> result = factory.create(w, h);

        Cursor<FloatType> srcCursor = input.cursor();
        Cursor<UnsignedShortType> dstCursor = result.cursor();
        while (srcCursor.hasNext() && dstCursor.hasNext()) {
            dstCursor.next().set((short)srcCursor.next().get());
        }
        return result;
    }

    /**
     * Read an image array from an ImageJ ImagePlus object.
     *
     * @param imagePlus
     * @return
     */
    public static Img<?> fromImagePlus(ImagePlus imagePlus) {
        ImageType type = ImageType.fromImagePlusType(imagePlus.getType());
        int width = imagePlus.getWidth();
        int height = imagePlus.getHeight();
        int depth = imagePlus.getStackSize();

        ImageProcessor[] iplist = new ImageProcessor[depth];
        if (depth <= 1) {
            depth = 1;
            iplist[0] = imagePlus.getProcessor();
        }
        else {
            ImageStack stack = imagePlus.getStack();
            for (int i = 0; i < depth; i++) {
                iplist[i] = stack.getProcessor(i+1);
            }
        }
        switch (type) {
            case GRAY8: {
                ArrayImgFactory<ByteType> factory = new ArrayImgFactory<>(new ByteType());
                Img<ByteType> imp = factory.create(width, height, depth);
                Cursor<ByteType> cursor = imp.cursor();
                for (int i = 0; i < depth; i++) {
                    byte[] array = (byte[]) iplist[i].getPixels();
                    int idx = 0;
                    while (cursor.hasNext() && idx < array.length) {
                        cursor.next().set(array[idx++]);
                    }
                }
                return imp;
            }
            case GRAY16: {
                ArrayImgFactory<UnsignedShortType> factory = new ArrayImgFactory<>(new UnsignedShortType());
                Img<UnsignedShortType> imp = factory.create(width, height, depth);
                Cursor<UnsignedShortType> cursor = imp.cursor();
                for (int i = 0; i < depth; i++) {
                    short[] array = (short[]) iplist[i].getPixels();
                    int idx = 0;
                    while (cursor.hasNext() && idx < array.length) {
                        cursor.next().set(array[idx++]);
                    }
                }
                return imp;
            }
            case RGB: {
                ArrayImgFactory<ARGBType> factory = new ArrayImgFactory<>(new ARGBType());
                Img<ARGBType> imp = factory.create(width, height, depth);
                Cursor<ARGBType> cursor = imp.cursor();
                for (int i = 0; i < depth; i++) {
                    int[] array = (int[]) iplist[i].getPixels();
                    int idx = 0;
                    while (cursor.hasNext() && idx < array.length) {
                        int argb = 0xFF000000 | array[idx];
                        cursor.next().set(argb);
                        idx++;
                    }
                }
                return imp;
            }
            default:
                throw new IllegalArgumentException("Unsupported image type: " + type);
        }
    }

    /**
     * Read an image from a byte stream.
     *
     * @param title  image title
     * @param name   image (file) name used only for determining the image encoding
     * @param stream image pixels stream
     * @return
     * @throws Exception
     */
    public static Img<?> readImg(String title, String name, InputStream stream) throws Exception {
        ImageArrayUtils.ImageFormat format = getImageFormat(name);
        ImagePlus imagePlus;
        long start, end;
        start = System.currentTimeMillis();
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
        end = System.currentTimeMillis();
        //System.out.println("readImagePlus time: "+((float)(end-start)/1000)+"sec");
        try {
            return fromImagePlus(imagePlus);
        } finally {
            imagePlus.close();
        }
    }

    public static Img<?> loadImageFromFileData(FileData fd) {
        long startTime = System.currentTimeMillis();
        InputStream inputStream;
        try {
            inputStream = openInputStream(fd);
            if (inputStream == null) {
                return null;
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        try {
            return ImgUtils.readImg(fd.getName(), fd.getName(), inputStream);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            try {
                inputStream.close();
            } catch (IOException ignore) {
            }
        }
    }

    @Nullable
    public static InputStream openInputStream(FileData fileData) throws IOException {
        if (fileData == null) {
            return null;
        } else if (fileData.getDataType() == FileData.FileDataType.zipEntry) {
            Path dataPath = Paths.get(fileData.getFileName());
            if (Files.isDirectory(dataPath)) {
                return openFileStream(dataPath.resolve(fileData.getEntryName()));
            } else if (Files.isRegularFile(dataPath)) {
                return openZipEntryStream(dataPath, fileData.getEntryName());
            } else {
                return null;
            }
        } else {
            Path dataPath = Paths.get(fileData.getFileName());
            if (Files.isDirectory(dataPath)) {
                return openFileStream(dataPath.resolve(fileData.getEntryName()));
            } else if (Files.isRegularFile(dataPath)) {
                return openFileStream(dataPath);
            } else {
                return null;
            }
        }
    }

    private static InputStream openFileStream(Path fp) throws IOException {
        return Files.newInputStream(fp);
    }

    private static InputStream openZipEntryStream(Path zipFilePath, String entryName) throws IOException {
        ZipFile archiveFile = new ZipFile(zipFilePath.toFile());
        ZipEntry ze = archiveFile.getEntry(entryName);
        if (ze != null) {
            return archiveFile.getInputStream(ze);
        } else {
            String imageFn = Paths.get(entryName).getFileName().toString();
            return archiveFile.stream()
                    .filter(aze -> !aze.isDirectory())
                    .filter(aze -> imageFn.equals(Paths.get(aze.getName()).getFileName().toString()))
                    .findFirst()
                    .map(aze -> getEntryStream(archiveFile, aze))
                    .orElseGet(() -> {
                        try {
                            archiveFile.close();
                        } catch (IOException ignore) {
                        }
                        return null;
                    });
        }
    }

    private static InputStream getEntryStream(ZipFile archiveFile, ZipEntry zipEntry) {
        try {
            return archiveFile.getInputStream(zipEntry);
        } catch (IOException e) {
            return null;
        }
    }
}
