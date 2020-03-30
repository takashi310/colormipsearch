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
import java.util.List;
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

    private static class ResultsFileHandler {
        private final RandomAccessFile rf;
        private final FileLock fl;
        private final OutputStream fs;

        ResultsFileHandler(RandomAccessFile rf, FileLock fl, OutputStream fs) {
            this.rf = rf;
            this.fl = fl;
            this.fs = fs;
        }

        void close() {
            if (fl != null) {
                try {
                    fl.release();
                } catch (IOException ignore) {
                }
            }
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException ignore) {
                }
            }
            if (rf != null) {
                try {
                    rf.close();
                } catch (IOException ignore) {
                }
            }
        }
    }

    private final String gradientMasksPath;
    private final String outputPath;
    private final Integer dataThreshold;
    private final Integer maskThreshold;
    private final Integer xyShift;
    private final boolean mirrorMask;
    private final Double pixColorFluctuation;
    private final Double pctPositivePixels;
    private final EM2LMAreaGapCalculator gradientBasedScoreAdjuster;

    ColorMIPSearch(String gradientMasksPath,
                   String outputPath,
                   Integer dataThreshold,
                   Integer maskThreshold,
                   Double pixColorFluctuation,
                   Integer xyShift,
                   int negativeRadius,
                   boolean mirrorMask,
                   Double pctPositivePixels) {
        this.gradientMasksPath = gradientMasksPath;
        this.outputPath =  outputPath;
        this.dataThreshold = dataThreshold;
        this.maskThreshold = maskThreshold;
        this.pixColorFluctuation = pixColorFluctuation;
        this.xyShift = xyShift;
        this.mirrorMask = mirrorMask;
        this.pctPositivePixels = pctPositivePixels;
        this.gradientBasedScoreAdjuster = new EM2LMAreaGapCalculator(maskThreshold, negativeRadius, mirrorMask);
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

    abstract void compareEveryMaskWithEveryLibrary(List<MIPInfo> maskMIPS, List<MIPInfo> libraryMIPS);

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
        if (sr.isMatch) {
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
        return Comparator.comparingInt(ColorMIPSearchResult::getMatchingSlices).reversed();
    }

    void writeSearchResults(String filename, List<ColorMIPSearchResultMetadata> searchResults) {
        long startTime = System.currentTimeMillis();
        if (CollectionUtils.isEmpty(searchResults)) {
            // nothing to do
            return;
        }
        ObjectMapper mapper = new ObjectMapper();
        if (StringUtils.isBlank(filename)) {
            try {
                JsonGenerator gen = mapper.getFactory().createGenerator(System.out, JsonEncoding.UTF8);
                writeColorSearchResults(gen, searchResults);
            } catch (IOException e) {
                LOG.error("Error writing json output for {} results", searchResults.size(), e);
                throw new UncheckedIOException(e);
            } finally {
                LOG.info("Written {} results in {}ms", searchResults.size(), System.currentTimeMillis() - startTime);
            }
        } else {
            ResultsFileHandler rfHandler;
            JsonGenerator gen;
            File outputFile = StringUtils.isBlank(outputPath) ? new File(filename + ".json") : new File(outputPath, filename + ".json");
            long initialOutputFileSize;
            try {
                rfHandler = openFile(outputFile);
            } catch (IOException e) {
                LOG.error("Error opening the outputfile {}", outputFile, e);
                throw new UncheckedIOException(e);
            }
            try {
                initialOutputFileSize = rfHandler.rf.length();
                gen = mapper.getFactory().createGenerator(rfHandler.fs, JsonEncoding.UTF8);
                gen.useDefaultPrettyPrinter();
            } catch (IOException e) {
                rfHandler.close();
                LOG.error("Error creating the JSON writer for writing {} results", searchResults.size(), e);
                throw new UncheckedIOException(e);
            }
            if (initialOutputFileSize > 0) {
                try {
                    LOG.info("Append {} results to {}", searchResults.size(), outputFile);
                    // FP is positioned at the end of the last element
                    // position FP after the end of the last item
                    // this may not work on Windows because of the new line separator
                    // - so on windows it may need to rollback more than 4 chars
                    long endOfLastItemPos = initialOutputFileSize - 4;
                    rfHandler.rf.seek(endOfLastItemPos);
                    rfHandler.fs.write(", ".getBytes()); // write the separator for the next array element
                    // append the new elements to the existing results
                    gen.writeStartObject(); // just to tell the generator that this is inside of an object which has an array
                    gen.writeArrayFieldStart("results");
                    gen.writeObject(searchResults.get(0)); // write the first element - it can be any element or dummy object
                    // just to fool the generator that there is already an element in the array
                    gen.flush();
                    // reset the position
                    rfHandler.rf.seek(endOfLastItemPos);
                    // and now start writing the actual elements
                    writeColorSearchResultsArray(gen, searchResults);
                    gen.writeEndArray();
                    gen.writeEndObject();
                    gen.flush();
                    long currentPos = rfHandler.rf.getFilePointer();
                    rfHandler.rf.setLength(currentPos); // truncate
                } catch (IOException e) {
                    LOG.error("Error writing json output for {} results to existing outputfile {}", searchResults.size(), outputFile, e);
                    throw new UncheckedIOException(e);
                } finally {
                    closeFile(rfHandler);
                    LOG.info("Written {} results to existing file -> {} in {}ms", searchResults.size(), outputFile, System.currentTimeMillis() - startTime);
                }
            } else {
                try {
                    LOG.info("Create {} with {} results", outputFile, searchResults.size());
                    writeColorSearchResults(gen, searchResults);
                } catch (IOException e) {
                    LOG.error("Error writing json output for {} results to new outputfile {}", searchResults.size(), outputFile, e);
                    throw new UncheckedIOException(e);
                } finally {
                    closeFile(rfHandler);
                    LOG.info("Written {} results to new file -> {} in {}ms", searchResults.size(), outputFile, System.currentTimeMillis() - startTime);
                }
            }
        }
    }

    private ResultsFileHandler openFile(File f) throws IOException {
        long startTime = System.currentTimeMillis();
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        FileChannel fc = rf.getChannel();
        try {
            FileLock fl = fc.tryLock();
            if (fl == null) {
                throw new IllegalStateException("Could not acquire lock for " + f);
            } else {
                LOG.info("Obtained the lock for {} in {}ms", f, System.currentTimeMillis() - startTime);
                return new ResultsFileHandler(rf, fl, Channels.newOutputStream(fc));
            }
        } catch (OverlappingFileLockException e) {
            throw new IllegalStateException("Could not acquire lock for " + f, e);
        }
    }

    private void closeFile(ResultsFileHandler rfh) {
        rfh.close();
    }

    private void writeColorSearchResults(JsonGenerator gen, List<ColorMIPSearchResultMetadata> searchResults) throws IOException {
        gen.useDefaultPrettyPrinter();
        gen.writeStartObject();
        gen.writeArrayFieldStart("results");
        writeColorSearchResultsArray(gen, searchResults);
        gen.writeEndArray();
        gen.writeEndObject();
        gen.flush();
    }

    private void writeColorSearchResultsArray(JsonGenerator gen, List<ColorMIPSearchResultMetadata> searchResults) throws IOException {
        for (ColorMIPSearchResultMetadata sr : searchResults) {
            gen.writeObject(sr);
        }
    }

    void terminate() {
    }
}
