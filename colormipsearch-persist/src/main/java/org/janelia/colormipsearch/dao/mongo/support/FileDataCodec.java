package org.janelia.colormipsearch.dao.mongo.support;

import java.io.UncheckedIOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.bson.BsonReader;
import org.bson.BsonSerializationException;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.janelia.colormipsearch.model.FileData;

public class FileDataCodec implements Codec<FileData> {

    private final ObjectMapper objectMapper;

    FileDataCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void encode(final BsonWriter writer, final FileData value, final EncoderContext encoderContext) {
        try {
            writer.writeString(objectMapper.writeValueAsString(value));
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Class<FileData> getEncoderClass() {
        return FileData.class;
    }

    @Override
    public FileData decode(final BsonReader reader, final DecoderContext decoderContext) {
        return objectMapper.convertValue(reader.readString(), FileData.class);
    }
}
