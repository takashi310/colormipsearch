package org.janelia.colormipsearch.model.json;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import org.janelia.colormipsearch.model.FileData;

public class FileDataDeserializer extends JsonDeserializer<FileData> {
    @Override
    public FileData deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        if (p.getCurrentToken() == JsonToken.START_OBJECT || p.getCurrentToken() == JsonToken.VALUE_NULL) {
            return p.readValueAs(FileData.class);
        } else if (p.getCurrentToken() == JsonToken.VALUE_STRING) {
            return FileData.fromString(p.getValueAsString());
        } else {
            return null;
        }
    }
}
