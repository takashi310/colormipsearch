package org.janelia.colormipsearch.dao.mongo.support;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.bson.BsonReader;
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
        if (value == null) {
            writer.writeNull();
        } else if (value.getDataType() == FileData.FileDataType.zipEntry) {
            writer.writeStartDocument();
            writer.writeString("dataType", value.getDataType().toString());
            writer.writeString("fileName", value.getFileName());
            writer.writeString("entryName", value.getEntryName());
            writer.writeEndDocument();
        } else {
            writer.writeString(value.getFileName());
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
