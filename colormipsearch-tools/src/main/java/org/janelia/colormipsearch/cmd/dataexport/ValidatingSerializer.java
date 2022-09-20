package org.janelia.colormipsearch.cmd.dataexport;

import java.io.IOException;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class ValidatingSerializer extends JsonSerializer<Object> {
    private final JsonSerializer<Object> defaultSerializer;
    private final Validator validator;

    public ValidatingSerializer(Validator validator, JsonSerializer<Object> defaultSerializer) {
        this.defaultSerializer = defaultSerializer;
        this.validator = validator;
    }

    @Override
    public void serialize(Object v, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        Set<ConstraintViolation<Object>> violations = validator.validate(v);
        if (!violations.isEmpty()) {
            throw new IOException("Validation exception for " + v + ": " + violations);
        }
        defaultSerializer.serialize(v, jsonGenerator, serializerProvider);
    }
}
