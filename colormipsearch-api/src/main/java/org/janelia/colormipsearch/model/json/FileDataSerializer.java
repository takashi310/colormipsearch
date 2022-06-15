package org.janelia.colormipsearch.model.json;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import org.janelia.colormipsearch.model.FileData;

public class FileDataSerializer extends JsonSerializer<FileData> {
    @Override
    public void serialize(FileData value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        if (value == null) {
            gen.writeNull();
        } if (value.getDataType() == FileData.FileDataType.zipEntry) {
            gen.writeObject(value);
        } else {
            gen.writeString(value.getFileName());
        }
    }
}
