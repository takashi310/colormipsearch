package org.janelia.colormipsearch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;

import javax.imageio.ImageIO;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import ij.ImagePlus;
import ij.io.Opener;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
abstract class ColorMIPSearch implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ColorMIPSearch.class);
    private static final int MAX_GRAD_DEPTH = 5;

    private enum ImageFormat {
        PNG,
        TIFF,
        UNKNOWN
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
        mip.cdmFilepath = mip.imageFilepath = mipPath.toString();
        return loadMIP(mip);
    }

    MIPImage loadMIP(MIPInfo mip) {
        long startTime = System.currentTimeMillis();
        LOG.debug("Load MIP {}", mip);
        InputStream inputStream;
        try {
            inputStream = new FileInputStream(mip.imageFilepath);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        ImagePlus ij = null;
        try {
            ij = readImagePlus(mip.id, getImageFormat(mip.imageFilepath), inputStream);
            return new MIPImage(mip, ij);
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
            Path mipPath = Paths.get(mipInfo.imageFilepath);
            String gradientFilename = StringUtils.replacePattern(mipPath.getFileName().toString(), "\\.tif(f)?$", ".png");
            try {
                return Files.find(gradientBasePath, MAX_GRAD_DEPTH, (p, fa) -> p.getFileName().toString().equals(gradientFilename)).findFirst()
                        .map(gp -> {
                            LOG.debug("Read gradient for {} from {}", mipInfo, gp);
                            return loadMIPFromPath(gp);
                        })
                        .orElse(null);
            } catch (IOException e) {
                LOG.error("Error reading {}", gradientBasePath, e);
                return null;
            }
        }
    }

    abstract void compareEveryMaskWithEveryLibrary(List<MIPInfo> maskMIPS, List<MIPInfo> libraryMIPS);

    ColorMIPSearchResult runImageComparison(MIPImage libraryMIPImage, MIPImage maskMIPImage) {
        long startTime = System.currentTimeMillis();
        try {
            LOG.debug("Compare library file {} with mask {}", libraryMIPImage,  maskMIPImage);
            double pixfludub = pixColorFluctuation / 100;

            final ColorMIPMaskCompare cc = new ColorMIPMaskCompare(
                    maskMIPImage,
                    maskThreshold,
                    mirrorMask,
                    null,
                    0,
                    mirrorMask,
                    dataThreshold,
                    pixfludub,
                    xyShift
            );
            ColorMIPMaskCompare.Output output = cc.runSearch(libraryMIPImage);

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

    ColorMIPSearchResult applyGradientAreaAdjustment(ColorMIPSearchResult sr, MIPImage libraryMIPImage, MIPImage libraryGradientImage, MIPImage maskMIPImage, MIPImage maskGradientImage) {
        ColorMIPSearchResult.AreaGap areaGap;
        if (sr.isMatch) {
            long startTime = System.currentTimeMillis();
            if (maskMIPImage.mipInfo.isEmSkelotonMIP()) {
                areaGap = gradientBasedScoreAdjuster.calculateAdjustedScore(libraryMIPImage, maskMIPImage, libraryGradientImage);
                LOG.debug("Completed calculating area gap between {} and {} with {} in {}ms", libraryMIPImage, maskMIPImage, libraryGradientImage, System.currentTimeMillis() - startTime);
            } else {
                areaGap = gradientBasedScoreAdjuster.calculateAdjustedScore(maskMIPImage, libraryMIPImage, maskGradientImage);
                LOG.debug("Completed calculating area gap between {} and {} with {} in {}ms", libraryMIPImage, maskMIPImage, maskGradientImage, System.currentTimeMillis() - startTime);
            }
            return sr.applyGradientAreaGap(areaGap);
        } else {
            return sr;
        }
    }

    Comparator<ColorMIPSearchResult> getColorMIPSearchComparator() {
        return Comparator.comparingInt(ColorMIPSearchResult::getMatchingSlices).reversed();
    }

    synchronized void writeSearchResults(String filename, List<ColorMIPSearchResultMetadata> searchResults) {
        long startTime = System.currentTimeMillis();

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
            FileLock rfl;
            OutputStream outputStream;
            JsonGenerator gen;
            File outputFile = StringUtils.isBlank(outputPath) ? new File(filename + ".json") : new File(outputPath, filename + ".json");
            long initialOutputFileSize;
            try {
                rfl = openFile(outputFile);
                initialOutputFileSize = rfl.channel().size();
                outputStream = Channels.newOutputStream(rfl.channel());
                gen = mapper.getFactory().createGenerator(outputStream, JsonEncoding.UTF8);
                gen.useDefaultPrettyPrinter();
            } catch (IOException e) {
                LOG.error("Error opening the outputfile {}", outputFile, e);
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
                    rfl.channel().position(endOfLastItemPos);
                    outputStream.write(", ".getBytes()); // write the separator for the next array element
                    // append the new elements to the existing results
                    gen.writeStartObject(); // just to tell the generator that this is inside of an object which has an array
                    gen.writeArrayFieldStart("results");
                    gen.writeObject(searchResults.get(0)); // write the first element - it can be any element or dummy object
                    // just to fool the generator that there is already an element in the array
                    gen.flush();
                    // reset the position
                    rfl.channel().position(endOfLastItemPos);
                    // and now start writing the actual elements
                    writeColorSearchResultsArray(gen, searchResults);
                    gen.writeEndArray();
                    gen.writeEndObject();
                    gen.flush();
                    long currentPos = rfl.channel().position();
                    rfl.channel().truncate(currentPos); // truncate
                } catch (IOException e) {
                    LOG.error("Error writing json output for {} results to existing outputfile {}", searchResults.size(), outputFile, e);
                    throw new UncheckedIOException(e);
                } finally {
                    try {
                        rfl.release();
                    } catch (IOException ignore) {
                    }
                    try {
                        outputStream.close();
                    } catch (IOException ignore) {
                    }
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
                    try {
                        rfl.release();
                    } catch (IOException ignore) {
                    }
                    try {
                        outputStream.close();
                    } catch (IOException ignore) {
                    }
                    LOG.info("Written {} results to new file -> {} in {}ms", searchResults.size(), outputFile, System.currentTimeMillis() - startTime);
                }
            }
        }
    }

    private synchronized FileLock openFile(File f) throws IOException {
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        FileChannel fc = rf.getChannel();
        return fc.lock();
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
