package org.janelia.colormipsearch;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nullable;
import javax.imageio.ImageIO;

import ij.ImagePlus;
import ij.io.Opener;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.janelia.colormipsearch.imageprocessing.ImageArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MIPsUtils {

    private enum ImageFormat {
        PNG,
        TIFF,
        UNKNOWN
    }

    private static final Logger LOG = LoggerFactory.getLogger(MIPsUtils.class);

    @Nullable
    static MIPImage loadMIP(@Nullable MIPInfo mip) {
        long startTime = System.currentTimeMillis();
        if (mip == null) {
            return null;
        } else {
            LOG.trace("Load MIP {}", mip);
            InputStream inputStream;
            try {
                inputStream = mip.openInputStream();
                if (inputStream == null) {
                    return null;
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            ImagePlus ij = null;
            try {
                ij = readImagePlus(mip.id, getImageFormat(mip.imagePath), inputStream);
                return new MIPImage(mip, new ImageArray(ij));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            } finally {
                try {
                    inputStream.close();
                } catch (IOException ignore) {
                }
                if (ij != null) {
                    ij.close();
                }
                LOG.trace("Loaded MIP {} in {}ms", mip, System.currentTimeMillis() - startTime);
            }
        }
    }

    /**
     * TransformedMIP can be the corresponding gradient image or a ZGap image that has applied the dilation already.
     * The typical pattern is that the image file name is the same but the path to it has a certain suffix
     * such as '_gradient' or '_20pxRGBMAX'
     * @param mipInfo
     * @param transformedMIPLocation
     * @param transformationLookupSuffix
     * @return
     */
    @Nullable
    static MIPInfo getTransformedMIPInfo(MIPInfo mipInfo, String transformedMIPLocation, String transformationLookupSuffix) {
        if (StringUtils.isBlank(transformedMIPLocation)) {
            return null;
        } else {
            Path transformedMIPPath = Paths.get(transformedMIPLocation);
            if (Files.isDirectory(transformedMIPPath)) {
                return getTransformedMIPInfoFromFilePath(transformedMIPPath, Paths.get(mipInfo.imagePath), transformationLookupSuffix);
            } else if (Files.isRegularFile(transformedMIPPath) && StringUtils.endsWithIgnoreCase(transformedMIPLocation, ".zip")) {
                return getTransformedMIPInfoFromZipEntry(transformedMIPLocation, mipInfo.imagePath, transformationLookupSuffix);
            } else {
                return null;
            }
        }
    }

    @Nullable
    private static MIPInfo getTransformedMIPInfoFromFilePath(Path transformedMIPPath, Path mipPath, String transformationLookupSuffix) {
        Path mipParentPath = mipPath.getParent();
        String transformedMIPFilename = StringUtils.replacePattern(mipPath.getFileName().toString(), "\\.tif(f)?$", ".png");
        List<Path> transformedMIPPaths;
        if (mipParentPath == null) {
            transformedMIPPaths = Collections.singletonList(transformedMIPPath.resolve(transformedMIPFilename));
        } else {
            int nComponents = mipParentPath.getNameCount();
            transformedMIPPaths = IntStream.range(1, nComponents)
                    .map(i -> nComponents - 1)
                    .mapToObj(i -> mipParentPath.getName(i).toString() + transformationLookupSuffix)
                    .reduce(new ArrayList<String>(),
                            (a, e) -> {
                                if (a.isEmpty()) {
                                    a.add(e);
                                } else {
                                    String lastElement = a.get(a.size() - 1);
                                    a.add(lastElement + "/" + e);
                                }
                                return a;
                            },
                            (a1, a2) -> {
                                a1.addAll(a2);
                                return a1;
                            })
                    .stream()
                    .map(p -> transformedMIPPath.resolve(p).resolve(transformedMIPFilename))
                    .collect(Collectors.toList());
        }
        Path transformedMIPImagePath = transformedMIPPaths.stream().filter(p -> Files.exists(p)).filter(p -> Files.isRegularFile(p)).findFirst().orElse(null);
        if (transformedMIPImagePath == null) {
            return null;
        } else {
            MIPInfo transformedMIP = new MIPInfo();
            transformedMIP.cdmPath = transformedMIP.imagePath = transformedMIPImagePath.toString();
            return transformedMIP;
        }
    }

    @Nullable
    private static MIPInfo getTransformedMIPInfoFromZipEntry(String transformedMIPLocation, String mipEntryName, String transformationLookupSuffix) {
        String transformedMIPFilename = StringUtils.replacePattern(mipEntryName, "\\.tif(f)?$", ".png");
        Path transformedMIPEntryPath = Paths.get(transformedMIPFilename);
        int nComponents = transformedMIPEntryPath.getNameCount();
        String transformedMIPEntryName = IntStream.range(0, nComponents)
                .mapToObj(i -> i < nComponents-1 ? transformedMIPEntryPath.getName(i).toString() + transformationLookupSuffix : transformedMIPEntryPath.getName(i).toString())
                .reduce("", (p, pc) -> StringUtils.isBlank(p) ? pc : p + "/" + pc);
        MIPInfo transformedMIP = new MIPInfo();
        transformedMIP.type = "zipEntry";
        transformedMIP.archivePath = transformedMIPLocation;
        transformedMIP.cdmPath = transformedMIPEntryName;
        transformedMIP.imagePath = transformedMIPEntryName;
        return transformedMIP;
    }

    private static ImagePlus readImagePlus(String title, ImageFormat format, InputStream stream) throws Exception {
        switch (format) {
            case PNG:
                return readPngToImagePlus(title, stream);
            case TIFF:
                return readTiffToImagePlus(title, stream);
        }
        throw new IllegalArgumentException("Image must be in PNG or TIFF format");
    }

    private static ImageFormat getImageFormat(String filepath) {
        String lowerPath = filepath.toLowerCase();

        if (lowerPath.endsWith(".png")) {
            return ImageFormat.PNG;
        } else if (lowerPath.endsWith(".tiff") || lowerPath.endsWith(".tif")) {
            return ImageFormat.TIFF;
        }

        LOG.warn("Image format unknown: {}", filepath);
        return ImageFormat.UNKNOWN;
    }

    private static ImagePlus readPngToImagePlus(String title, InputStream stream) throws Exception {
        return new ImagePlus(title, ImageIO.read(stream));
    }

    private static ImagePlus readTiffToImagePlus(String title, InputStream stream) throws Exception {
        return new Opener().openTiff(stream, title);
    }

}
