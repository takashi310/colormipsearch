package org.janelia.colormipsearch.dataio.fs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.janelia.colormipsearch.dataio.CDSDataInputGenerator;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONCDSDataInputGenerator implements CDSDataInputGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(JSONCDSDataInputGenerator.class);

    private final ObjectMapper mapper;
    private final JsonGenerator gen;

    public JSONCDSDataInputGenerator(Path outputPath,
                                     String outputFileName,
                                     int libraryFromIndex,
                                     int libraryToIndex,
                                     boolean append,
                                     ObjectMapper mapper) {
        String outputName = libraryFromIndex > 0
                ? outputFileName + "-" + libraryFromIndex + "-" + libraryToIndex + ".json"
                : outputFileName + ".json";
        this.mapper = mapper;
        this.gen = createJsonGenerator(outputPath, outputName, append);
    }

    public CDSDataInputGenerator prepare() {
        return this;
    }

    @Override
    public void write(AbstractNeuronMetadata neuronMetadata) {
        try {
            gen.writeObject(neuronMetadata);
        } catch (Exception e) {
            LOG.error("Error writing {}", neuronMetadata, e);
        }
    }

    @Override
    public void done() {
        try {
            gen.writeEndArray();
            gen.flush();
        } catch (Exception e) {
            LOG.error("Error finalizing CDS data generator for {}", gen.getOutputTarget(), e);
        }
    }

    private JsonGenerator createJsonGenerator(Path outputPath,
                                              String outputName,
                                              boolean append) {
        try {
            Files.createDirectories(outputPath);
        } catch (Exception e) {
            throw new IllegalStateException("Error creating output directory: " + outputPath, e);
        }
        Path outputFilePath = outputPath.resolve(outputName);
        LOG.info("Write color depth MIPs to {}", outputFilePath);
        if (Files.exists(outputFilePath) && append) {
            return openOutputForAppend(outputFilePath.toFile());
        } else {
            return openOutput(outputFilePath.toFile());
        }
    }

    private JsonGenerator openOutputForAppend(File of) {
        try {
            LOG.debug("Append to {}", of);
            RandomAccessFile rf = new RandomAccessFile(of, "rw");
            long rfLength = rf.length();
            // position FP after the end of the last item
            // this may not work on Windows because of the new line separator
            // - so on windows it may need to rollback more than 4 chars
            rf.seek(rfLength - 2);
            OutputStream outputStream = Channels.newOutputStream(rf.getChannel());
            outputStream.write(',');
            long pos = rf.getFilePointer();
            JsonGenerator gen = mapper.getFactory().createGenerator(outputStream, JsonEncoding.UTF8);
            gen.useDefaultPrettyPrinter();
            gen.writeStartArray();
            gen.flush();
            rf.seek(pos);
            return gen;
        } catch (IOException e) {
            LOG.error("Error creating the output stream to be appended for {}", of, e);
            throw new UncheckedIOException(e);
        }
    }

    private JsonGenerator openOutput(File of) {
        try {
            JsonGenerator gen = mapper.getFactory().createGenerator(new FileOutputStream(of), JsonEncoding.UTF8);
            gen.useDefaultPrettyPrinter();
            gen.writeStartArray();
            return gen;
        } catch (IOException e) {
            LOG.error("Error creating the output stream for {}", of, e);
            throw new UncheckedIOException(e);
        }
    }

}
