package org.janelia.colormipsearch.cmd;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.janelia.colormipsearch.api.cdsearch.CDSMatches;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchMatchMetadata;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ColorMIPSearchResultsWriter {
    private static final Logger LOG = LoggerFactory.getLogger(ColorMIPSearchResultsWriter.class);

    private final static RetryPolicy<List<ColorMIPSearchResult>> RETRY_POLICY = new RetryPolicy<List<ColorMIPSearchResult>>()
            .handle(IllegalStateException.class)
            .withDelay(Duration.ofMillis(500))
            .withMaxRetries(20);

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

    static void writeSearchResults(Path outputPath, List<CDSMatches> cdsMatchesList) {
        LOG.info("Write {} file results", cdsMatchesList.size());
        cdsMatchesList.stream().parallel()
                .forEach(cdsMatches -> {
                    Failsafe.with(RETRY_POLICY).run(
                            () -> writeSearchResultsToFile(
                                    outputPath == null
                                            ? null
                                            : outputPath.resolve(cdsMatches.getMaskId() + ".json"),
                                    cdsMatches)
                    );
                });
        LOG.info("Finished writing {} file results", cdsMatchesList.size());
    }

    private static void writeSearchResultsToFile(Path outputFile, CDSMatches searchResults) {
        long startTime = System.currentTimeMillis();
        if (searchResults.isEmpty()) {
            // nothing to do
            return;
        }
        ObjectMapper mapper = new ObjectMapper();
        if (outputFile == null) {
            try {
                JsonGenerator gen = mapper.getFactory().createGenerator(System.out, JsonEncoding.UTF8);
                writeColorDepthSearchMatches(gen, searchResults);
            } catch (IOException e) {
                LOG.error("Error writing json output for {} results", searchResults.results.size(), e);
                throw new UncheckedIOException(e);
            } finally {
                LOG.info("Written {} results in {}ms", searchResults.results.size(), System.currentTimeMillis() - startTime);
            }
        } else {
            ResultsFileHandler rfHandler;
            JsonGenerator gen;
            long initialOutputFileSize;
            try {
                rfHandler = openFile(outputFile.toFile());
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
                LOG.error("Error creating the JSON writer for writing {} results", searchResults.results.size(), e);
                throw new UncheckedIOException(e);
            }
            if (initialOutputFileSize > 0) {
                // result file exists and is not empty
                try {
                    LOG.info("Append {} results to {}", searchResults.results.size(), outputFile);
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
                    gen.writeObject(searchResults.results.get(0)); // write the first element - it can be any element or dummy object
                    // just to fool the generator that there is already an element in the array
                    gen.flush();
                    // reset the position
                    rfHandler.rf.seek(endOfLastItemPos);
                    // and now start writing the actual elements
                    writeColorSearchMatchesArray(gen, searchResults.results);
                    gen.writeEndArray();
                    gen.writeEndObject();
                    gen.flush();
                    long currentPos = rfHandler.rf.getFilePointer();
                    rfHandler.rf.setLength(currentPos); // truncate
                } catch (IOException e) {
                    LOG.error("Error writing json output for {} results to existing outputfile {}", searchResults.results.size(), outputFile, e);
                    throw new UncheckedIOException(e);
                } finally {
                    closeFile(rfHandler);
                    LOG.info("Written {} results to existing file -> {} in {}ms", searchResults.results.size(), outputFile, System.currentTimeMillis() - startTime);
                }
            } else {
                // new result file
                try {
                    LOG.info("Create {} with {} results", outputFile, searchResults.results.size());
                    writeColorDepthSearchMatches(gen, searchResults);
                } catch (IOException e) {
                    LOG.error("Error writing json output for {} results to new outputfile {}", searchResults.results.size(), outputFile, e);
                    throw new UncheckedIOException(e);
                } finally {
                    closeFile(rfHandler);
                    LOG.info("Written {} results to new file -> {} in {}ms", searchResults.results.size(), outputFile, System.currentTimeMillis() - startTime);
                }
            }
        }
    }

    private static ResultsFileHandler openFile(File f) throws IOException {
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

    private static void closeFile(ResultsFileHandler rfh) {
        rfh.close();
    }

    private static void writeColorDepthSearchMatches(JsonGenerator gen, CDSMatches cdsMatches) throws IOException {
        gen.useDefaultPrettyPrinter();
        gen.writeStartObject();
        gen.writeStringField("maskId", cdsMatches.getMaskId());
        gen.writeStringField("maskPublishedName", cdsMatches.getMaskPublishedName());
        gen.writeStringField("maskLibraryName", cdsMatches.getMaskLibraryName());
        gen.writeArrayFieldStart("results");
        writeColorSearchMatchesArray(gen, cdsMatches.results);
        gen.writeEndArray();
        gen.writeEndObject();
        gen.flush();
    }

    private static void writeColorSearchMatchesArray(JsonGenerator gen, List<ColorMIPSearchMatchMetadata> searchResults) throws IOException {
        for (ColorMIPSearchMatchMetadata sr : searchResults) {
            gen.writeObject(sr);
        }
    }

}
