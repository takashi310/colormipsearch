package org.janelia.colormipsearch.dao.mongo.support;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;

import org.janelia.colormipsearch.model.annotations.EntityId;

public class MongoNamingStrategy extends PropertyNamingStrategy {

    @SuppressWarnings("unchecked")
    @Override
    public String nameForField(MapperConfig<?> config, AnnotatedField field, String defaultName) {
        if (isIdAnnotatedField(field.getAnnotated())) {
            return "_id";
        } else {
            return super.nameForField(config, field, defaultName);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public String nameForGetterMethod(MapperConfig<?> config, AnnotatedMethod method, String defaultName) {
        return Arrays.stream(method.getDeclaringClass().getDeclaredFields())
                .filter(f -> f.getName().equals(defaultName))
                .filter(this::isIdAnnotatedField)
                .findFirst()
                .map(fname -> "_id")
                .orElseGet(() -> super.nameForGetterMethod(config, method, defaultName));
    }

    @SuppressWarnings("unchecked")
    @Override
    public String nameForSetterMethod(MapperConfig<?> config, AnnotatedMethod method, String defaultName) {
        return Arrays.stream(method.getDeclaringClass().getDeclaredFields())
                .filter(f -> f.getName().equals(defaultName))
                .filter(this::isIdAnnotatedField)
                .findFirst()
                .map(fname -> "_id")
                .orElseGet(() -> super.nameForSetterMethod(config, method, defaultName));
    }

    private boolean isIdAnnotatedField(Field f) {
        return f.isAnnotationPresent(EntityId.class);
    }
}
