package org.janelia.colormipsearch;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import javax.imageio.ImageIO;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import ij.ImagePlus;
import ij.io.Opener;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.imageprocessing.ImageArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
abstract class ColorMIPSearch implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ColorMIPSearch.class);

    private enum ImageFormat {
        PNG,
        TIFF,
        UNKNOWN
    }

    private final String gradientMasksPath;
    private final Integer dataThreshold;
    private final Integer maskThreshold;
    private final Integer xyShift;
    private final boolean mirrorMask;
    private final Double pixColorFluctuation;
    private final Double pctPositivePixels;
    private final int negativeRadius;
    private final EM2LMAreaGapCalculator gradientBasedScoreAdjuster;

    ColorMIPSearch(String gradientMasksPath,
                   Integer dataThreshold,
                   Integer maskThreshold,
                   Double pixColorFluctuation,
                   Integer xyShift,
                   int negativeRadius,
                   boolean mirrorMask,
                   Double pctPositivePixels) {
        this.gradientMasksPath = gradientMasksPath;
        this.dataThreshold = dataThreshold;
        this.maskThreshold = maskThreshold;
        this.pixColorFluctuation = pixColorFluctuation;
        this.xyShift = xyShift;
        this.mirrorMask = mirrorMask;
        this.pctPositivePixels = pctPositivePixels;
        this.negativeRadius = negativeRadius;
        this.gradientBasedScoreAdjuster = new EM2LMAreaGapCalculator(maskThreshold, negativeRadius, mirrorMask);
    }

    Map<String, String> getCDSParameters() {
        Map<String, String> cdsParams = new LinkedHashMap<>();
        cdsParams.put("gradientMasksPath", gradientMasksPath);
        cdsParams.put("dataThreshold", dataThreshold != null ? dataThreshold.toString() : null);
        cdsParams.put("maskThreshold", maskThreshold != null ? maskThreshold.toString() : null);
        cdsParams.put("xyShift", xyShift != null ? xyShift.toString() : null);
        cdsParams.put("mirrorMask", String.valueOf(mirrorMask));
        cdsParams.put("pixColorFluctuation", pixColorFluctuation != null ? pixColorFluctuation.toString() : null);
        cdsParams.put("pctPositivePixels", pctPositivePixels != null ? pctPositivePixels.toString() : null);
        cdsParams.put("negativeRadius", String.valueOf(negativeRadius));
        return cdsParams;
    }

    MIPImage loadMIPFromPath(Path mipPath) {
        MIPInfo mip = new MIPInfo();
        mip.cdmPath = mip.imagePath = mipPath.toString();
        return loadMIP(mip);
    }

    MIPImage loadMIP(MIPInfo mip) {
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

    private ImagePlus readImagePlus(String title, ImageFormat format, InputStream stream) throws Exception {
        switch (format) {
            case PNG:
                return readPngToImagePlus(title, stream);
            case TIFF:
                return readTiffToImagePlus(title, stream);
        }
        throw new IllegalArgumentException("Image must be in PNG or TIFF format");
    }

    private ImageFormat getImageFormat(String filepath) {
        String lowerPath = filepath.toLowerCase();

        if (lowerPath.endsWith(".png")) {
            return ImageFormat.PNG;
        } else if (lowerPath.endsWith(".tiff") || lowerPath.endsWith(".tif")) {
            return ImageFormat.TIFF;
        }

        LOG.info("Image format unknown: {}", filepath);
        return ImageFormat.UNKNOWN;
    }

    private ImagePlus readPngToImagePlus(String title, InputStream stream) throws Exception {
        return new ImagePlus(title, ImageIO.read(stream));
    }

    private ImagePlus readTiffToImagePlus(String title, InputStream stream) throws Exception {
        return new Opener().openTiff(stream, title);
    }

    MIPImage loadGradientMIP(MIPInfo mipInfo) {
        if (StringUtils.isBlank(gradientMasksPath)) {
            return null;
        } else {
            Path gradientBasePath = Paths.get(gradientMasksPath);
            if (Files.isDirectory(gradientBasePath)) {
                return loadGradientMIPFromFilePath(gradientBasePath, Paths.get(mipInfo.imagePath));
            } else if (Files.isRegularFile(gradientBasePath) && StringUtils.endsWithIgnoreCase(gradientMasksPath, ".zip")) {
                return loadGradientMIPFromZipEntry(mipInfo.imagePath);
            } else {
                return null;
            }
        }
    }

    private MIPImage loadGradientMIPFromFilePath(Path gradientBasePath, Path mipPath) {
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
            return loadMIPFromPath(gradientImagePath);
        }
    }

    private MIPImage loadGradientMIPFromZipEntry(String mipEntryName) {
        String gradientFilename = StringUtils.replacePattern(mipEntryName, "\\.tif(f)?$", ".png");
        Path gradientEntryPath = Paths.get(gradientFilename);
        int nComponents = gradientEntryPath.getNameCount();
        String gradientEntryName = IntStream.range(0, nComponents)
                .mapToObj(i -> i < nComponents-1 ? gradientEntryPath.getName(i).toString() + "_gradient" : gradientEntryPath.getName(i).toString())
                .reduce("", (p, pc) -> StringUtils.isBlank(p) ? pc : p + "/" + pc);
        MIPInfo gradientMIP = new MIPInfo();
        gradientMIP.type = "zipEntry";
        gradientMIP.archivePath = gradientMasksPath;
        gradientMIP.cdmPath = gradientEntryName;
        gradientMIP.imagePath = gradientEntryName;
        return loadMIP(gradientMIP);
    }

    abstract List<ColorMIPSearchResult> findAllColorDepthMatches(List<MIPInfo> maskMIPS, List<MIPInfo> libraryMIPS);

    ColorMIPSearchResult runImageComparison(MIPImage libraryMIPImage, MIPImage maskMIPImage) {
        long startTime = System.currentTimeMillis();
        try {
            LOG.debug("Compare library file {} with mask {}", libraryMIPImage,  maskMIPImage);
            double pixfludub = pixColorFluctuation / 100;

            final ColorMIPMaskCompare cc = new ColorMIPMaskCompare(
                    maskMIPImage.imageArray,
                    maskThreshold,
                    mirrorMask,
                    null,
                    0,
                    mirrorMask,
                    dataThreshold,
                    pixfludub,
                    xyShift
            );
            ColorMIPMaskCompare.Output output = cc.runSearch(libraryMIPImage.imageArray);

            double pixThresdub = pctPositivePixels / 100;
            boolean isMatch = output.matchingPct > pixThresdub;

            return new ColorMIPSearchResult(maskMIPImage.mipInfo, libraryMIPImage.mipInfo, output.matchingPixNum, output.matchingPct, isMatch, false);
        } catch (Throwable e) {
            LOG.warn("Error comparing library file {} with mask {}", libraryMIPImage,  maskMIPImage, e);
            return new ColorMIPSearchResult(maskMIPImage.mipInfo, libraryMIPImage.mipInfo, 0, 0, false, true);
        } finally {
            LOG.debug("Completed comparing library file {} with mask {} in {}ms", libraryMIPImage,  maskMIPImage, System.currentTimeMillis() - startTime);
        }
    }

    ColorMIPSearchResult applyGradientAreaAdjustment(ColorMIPSearchResult sr, MIPImage libraryMIPImage, MIPImage libraryGradientImage, MIPImage patternMIPImage, MIPImage patternGradientImage) {
        if (sr.isMatch()) {
            return sr.applyGradientAreaGap(calculateGradientAreaAdjustment(libraryMIPImage, libraryGradientImage, patternMIPImage, patternGradientImage));
        } else {
            return sr;
        }
    }

    ColorMIPSearchResult.AreaGap calculateGradientAreaAdjustment(MIPImage libraryMIPImage, MIPImage libraryGradientImage, MIPImage patternMIPImage, MIPImage patternGradientImage) {
        ColorMIPSearchResult.AreaGap areaGap;
        long startTime = System.currentTimeMillis();
        if (patternMIPImage.mipInfo.isEmSkelotonMIP() || libraryGradientImage != null) {
            areaGap = gradientBasedScoreAdjuster.calculateAdjustedScore(libraryMIPImage, patternMIPImage, libraryGradientImage);
            LOG.debug("Completed calculating area gap between {} and {} with {} in {}ms", libraryMIPImage, patternMIPImage, libraryGradientImage, System.currentTimeMillis() - startTime);
        } else if (patternGradientImage != null) {
            areaGap = gradientBasedScoreAdjuster.calculateAdjustedScore(patternMIPImage, libraryMIPImage, patternGradientImage);
            LOG.debug("Completed calculating area gap between {} and {} with {} in {}ms", libraryMIPImage, patternMIPImage, patternGradientImage, System.currentTimeMillis() - startTime);
        } else {
            areaGap = null;
        }
        return areaGap;
    }

    Comparator<ColorMIPSearchResult> getColorMIPSearchComparator() {
        return Comparator.comparingInt(ColorMIPSearchResult::getMatchingPixels).reversed();
    }

    void terminate() {
    }
}
