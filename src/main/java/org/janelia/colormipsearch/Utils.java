package org.janelia.colormipsearch;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import javax.imageio.ImageIO;

import ij.ImagePlus;
import ij.io.Opener;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.imageprocessing.ImageArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    private enum ImageFormat {
        PNG,
        TIFF,
        UNKNOWN
    }

    static MIPImage loadMIPFromPath(Path mipPath) {
        MIPInfo mip = new MIPInfo();
        mip.cdmPath = mip.imagePath = mipPath.toString();
        return loadMIP(mip);
    }

    static MIPImage loadMIP(MIPInfo mip) {
        long startTime = System.currentTimeMillis();
        LOG.debug("Load MIP {}", mip);
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
            LOG.debug("Loaded MIP {} in {}ms", mip, System.currentTimeMillis() - startTime);
        }
    }

    static MIPImage loadGradientMIP(MIPInfo mipInfo, String gradientsLocation) {
        if (StringUtils.isBlank(gradientsLocation)) {
            return null;
        } else {
            Path gradientBasePath = Paths.get(gradientsLocation);
            if (Files.isDirectory(gradientBasePath)) {
                return loadGradientMIPFromFilePath(gradientBasePath, Paths.get(mipInfo.imagePath));
            } else if (Files.isRegularFile(gradientBasePath) && StringUtils.endsWithIgnoreCase(gradientsLocation, ".zip")) {
                return loadGradientMIPFromZipEntry(gradientsLocation, mipInfo.imagePath);
            } else {
                return null;
            }
        }
    }

    private static MIPImage loadGradientMIPFromFilePath(Path gradientBasePath, Path mipPath) {
        Path parentGradientBasePath = gradientBasePath.getParent();
        if (parentGradientBasePath == null || !mipPath.startsWith(parentGradientBasePath)) {
            // don't know where to look for the gradient - I could try searching all subdirectories but is too expensive
            return null;
        }
        Path mipBasePath = parentGradientBasePath.relativize(mipPath);
        String gradientFilename = StringUtils.replacePattern(mipPath.getFileName().toString(), "\\.tif(f)?$", ".png");
        int nComponents = mipBasePath.getNameCount();
        Path gradientImagePath = IntStream.range(1, nComponents - 1)
                .mapToObj(i -> mipBasePath.getName(i).toString())
                .map(pc -> pc + "_gradient")
                .reduce(gradientBasePath, (p, pc) -> p.resolve(pc), (p1, p2) -> p1.resolve(p2))
                .resolve(gradientFilename)
                ;
        if (Files.notExists(gradientImagePath)) {
            return null;
        } else {
            return Utils.loadMIPFromPath(gradientImagePath);
        }
    }

    private static MIPImage loadGradientMIPFromZipEntry(String gradientsLocation, String mipEntryName) {
        String gradientFilename = StringUtils.replacePattern(mipEntryName, "\\.tif(f)?$", ".png");
        Path gradientEntryPath = Paths.get(gradientFilename);
        int nComponents = gradientEntryPath.getNameCount();
        String gradientEntryName = IntStream.range(0, nComponents)
                .mapToObj(i -> i < nComponents-1 ? gradientEntryPath.getName(i).toString() + "_gradient" : gradientEntryPath.getName(i).toString())
                .reduce("", (p, pc) -> StringUtils.isBlank(p) ? pc : p + "/" + pc);
        MIPInfo gradientMIP = new MIPInfo();
        gradientMIP.type = "zipEntry";
        gradientMIP.archivePath = gradientsLocation;
        gradientMIP.cdmPath = gradientEntryName;
        gradientMIP.imagePath = gradientEntryName;
        return Utils.loadMIP(gradientMIP);
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

        LOG.info("Image format unknown: {}", filepath);
        return ImageFormat.UNKNOWN;
    }

    private static ImagePlus readPngToImagePlus(String title, InputStream stream) throws Exception {
        return new ImagePlus(title, ImageIO.read(stream));
    }

    private static ImagePlus readTiffToImagePlus(String title, InputStream stream) throws Exception {
        return new Opener().openTiff(stream, title);
    }

    static <T> List<List<T>> partitionList(List<T> l, int partitionSize) {
        BiFunction<Pair<List<List<T>>, List<T>>, T, Pair<List<List<T>>, List<T>>> partitionAcumulator = (partitionResult, s) -> {
            List<T> currentPartition;
            if (partitionResult.getRight().size() == partitionSize) {
                currentPartition = new ArrayList<>();
            } else {
                currentPartition = partitionResult.getRight();
            }
            currentPartition.add(s);
            if (currentPartition.size() == 1) {
                partitionResult.getLeft().add(currentPartition);
            }
            return ImmutablePair.of(partitionResult.getLeft(), currentPartition);
        };
        return l.stream().reduce(
                ImmutablePair.of(new ArrayList<>(), new ArrayList<>()),
                partitionAcumulator,
                (r1, r2) -> r2.getLeft().stream().flatMap(p -> p.stream())
                        .map(s -> partitionAcumulator.apply(r1, s))
                        .reduce((first, second) -> second)
                        .orElse(r1)).getLeft()
                ;
    }

}
