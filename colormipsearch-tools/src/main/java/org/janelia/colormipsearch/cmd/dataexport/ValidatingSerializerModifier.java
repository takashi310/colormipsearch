package org.janelia.colormipsearch.cmd.dataexport;

import javax.validation.Validator;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;

public class ValidatingSerializerModifier extends BeanSerializerModifier {

    private final Validator validator;

    public ValidatingSerializerModifier(Validator validator) {
        this.validator = validator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public JsonSerializer<?> modifySerializer(SerializationConfig config, BeanDescription beanDesc, JsonSerializer<?> serializer) {
        return new ValidatingSerializer(validator, (JsonSerializer<Object>) serializer);
    }
}
