package org.janelia.colormipsearch.cmd.dataexport;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class ValidatingSerializer extends JsonSerializer<Object> {
    private final JsonSerializer<Object> defaultSerializer;

    public ValidatingSerializer(JsonSerializer<Object> defaultSerializer) {
        this.defaultSerializer = defaultSerializer;
    }

    @Override
    public void serialize(Object v, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        defaultSerializer.serialize(v, jsonGenerator, serializerProvider);
    }
}
