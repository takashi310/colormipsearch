package org.janelia.colormipsearch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.util.Comparator;
import java.util.List;

import javax.imageio.ImageIO;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import ij.ImagePlus;
import ij.io.Opener;
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

    private String outputPath;
    private Integer dataThreshold;
    private Integer xyShift;
    private boolean mirrorMask;
    private Double pixColorFluctuation;
    private Double pctPositivePixels;

    ColorMIPSearch(String outputPath,
                   Integer dataThreshold, Double pixColorFluctuation, Integer xyShift,
                   boolean mirrorMask, Double pctPositivePixels) {
        this.outputPath =  outputPath;
        this.dataThreshold = dataThreshold;
        this.pixColorFluctuation = pixColorFluctuation;
        this.xyShift = xyShift;
        this.mirrorMask = mirrorMask;
        this.pctPositivePixels = pctPositivePixels;
    }

    MIPWithPixels loadMIP(MIPInfo mip) {
        long startTime = System.currentTimeMillis();
        LOG.debug("Load MIP {}", mip);
        InputStream inputStream;
        try {
            inputStream = new FileInputStream(mip.filepath);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        ImagePlus ij = null;
        try {
            ij = readImagePlus(mip.id, getImageFormat(mip.filepath), inputStream);
            return new MIPWithPixels(mip, ij);
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

    abstract void compareEveryMaskWithEveryLibrary(List<MIPInfo> maskMIPS, List<MIPInfo> libraryMIPS, Integer maskThreshold);

    ColorMIPSearchResult runImageComparison(MIPWithPixels libraryMIP, MIPWithPixels maskMIP, Integer searchThreshold) {
        long startTime = System.currentTimeMillis();
        try {
            LOG.debug("Compare library file {} with mask {} using threshold {}", libraryMIP,  maskMIP, searchThreshold);
            double pixfludub = pixColorFluctuation / 100;

            final ColorMIPMaskCompare cc = new ColorMIPMaskCompare(
                    maskMIP,
                    searchThreshold,
                    mirrorMask,
                    null,
                    0,
                    mirrorMask,
                    dataThreshold,
                    pixfludub,
                    xyShift
            );
            ColorMIPMaskCompare.Output output = cc.runSearch(libraryMIP);

            double pixThresdub = pctPositivePixels / 100;
            boolean isMatch = output.matchingPct > pixThresdub;

            return new ColorMIPSearchResult(maskMIP, libraryMIP, output.matchingPixNum, output.matchingPct, isMatch, false);
        } catch (Throwable e) {
            LOG.warn("Error comparing library file {} with mask {}", libraryMIP,  maskMIP, e);
            return new ColorMIPSearchResult(maskMIP, libraryMIP, 0, 0, false, true);
        } finally {
            LOG.debug("Completed comparing library file {} with mask {} in {}ms", libraryMIP,  maskMIP, System.currentTimeMillis() - startTime);
        }
    }

    Comparator<ColorMIPSearchResult> getColorMIPSearchComparator() {
        return Comparator.comparingInt(ColorMIPSearchResult::getMatchingSlices).reversed();
    }

    void writeSearchResults(String filename, List<ColorMIPSearchResultMetadata> searchResults) {
        OutputStream outputStream;
        long startTime = System.currentTimeMillis();
        File outputFile = new File(outputPath, filename + ".json");
        LOG.info("Write {} results {} file -> {}", searchResults.size(), outputFile.exists() ? "existing" : "new", outputFile);

        ObjectMapper mapper = new ObjectMapper();
        if (outputFile.exists()) {
            LOG.debug("Append to {}", outputFile);
            RandomAccessFile rf;
            try {
                rf = new RandomAccessFile(outputFile, "rw");
                long rfLength = rf.length();
                // position FP after the end of the last item
                // this may not work on Windows because of the new line separator
                // - so on windows it may need to rollback more than 4 chars
                rf.seek(rfLength - 4);
                outputStream = Channels.newOutputStream(rf.getChannel());
            } catch (IOException e) {
                LOG.error("Error opening the outputfile {}", outputPath, e);
                throw new UncheckedIOException(e);
            }
            try {
                // FP is positioned at the end of the last element
                long endOfLastItemPos = rf.getFilePointer();
                outputStream.write(", ".getBytes()); // write the separator for the next array element
                // append the new elements to the existing results
                JsonGenerator gen = mapper.getFactory().createGenerator(outputStream, JsonEncoding.UTF8);
                gen.useDefaultPrettyPrinter();
                gen.writeStartObject(); // just to tell the generator that this is inside of an object which has an array
                gen.writeArrayFieldStart("results");
                gen.writeObject(searchResults.get(0)); // write the first element - it can be any element or dummy object
                                                       // just to fool the generator that there is already an element in the array
                gen.flush();
                // reset the position
                rf.seek(endOfLastItemPos);
                // and now start writing the actual elements
                writeColorSearchResultsArray(gen, searchResults);
                gen.writeEndArray();
                gen.writeEndObject();
                gen.flush();
                long currentPos = rf.getFilePointer();
                rf.setLength(currentPos); // truncate
            } catch (IOException e) {
                LOG.error("Error writing json output for {} results to existing outputfile {}", searchResults.size(), outputFile, e);
                throw new UncheckedIOException(e);
            } finally {
                try {
                    outputStream.close();
                } catch (IOException ignore) {
                }
                LOG.info("Written {} results {} file -> {} in {}ms", searchResults.size(), outputFile.exists() ? "existing" : "new", outputFile, System.currentTimeMillis() - startTime);
            }
        } else {
            try {
                outputStream = new FileOutputStream(outputFile);
            } catch (FileNotFoundException e) {
                LOG.error("Error opening the outputfile {}", outputPath, e);
                throw new UncheckedIOException(e);
            }
            try {
                JsonGenerator gen = mapper.getFactory().createGenerator(outputStream, JsonEncoding.UTF8);
                gen.useDefaultPrettyPrinter();
                gen.writeStartObject();
                gen.writeArrayFieldStart("results");
                writeColorSearchResultsArray(gen, searchResults);
                gen.writeEndArray();
                gen.writeEndObject();
                gen.flush();
            } catch (IOException e) {
                LOG.error("Error writing json output for {} results to new outputfile {}", searchResults.size(), outputFile, e);
                throw new UncheckedIOException(e);
            } finally {
                try {
                    outputStream.close();
                } catch (IOException ignore) {
                }
                LOG.info("Written {} results {} file -> {} in {}ms", searchResults.size(), outputFile.exists() ? "existing" : "new", outputFile, System.currentTimeMillis() - startTime);
            }
        }
    }

    private void writeColorSearchResultsArray(JsonGenerator gen, List<ColorMIPSearchResultMetadata> searchResults) throws IOException {
        for (ColorMIPSearchResultMetadata sr : searchResults) {
            gen.writeObject(sr);
        }
    }

    void terminate() {
    }
}
