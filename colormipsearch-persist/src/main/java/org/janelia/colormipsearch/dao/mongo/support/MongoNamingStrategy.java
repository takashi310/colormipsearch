package org.janelia.colormipsearch.dao.mongo.support;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;

public class MongoNamingStrategy extends PropertyNamingStrategy {
    private static final FieldHandler[] REGISTERED_FIELD_HANDLERS = new FieldHandler[] {
            new EntityIdFieldHandler()
    };

    @SuppressWarnings("unchecked")
    @Override
    public String nameForField(MapperConfig<?> config, AnnotatedField field, String defaultName) {
        for (FieldHandler fh : REGISTERED_FIELD_HANDLERS) {
            if (fh.supportsField(field.getAnnotated(), defaultName) && fh.nameForField(defaultName) != null) {
                return fh.nameForField(defaultName);
            }
        }
        return super.nameForField(config, field, defaultName);
    }

    @SuppressWarnings("unchecked")
    @Override
    public String nameForGetterMethod(MapperConfig<?> config, AnnotatedMethod method, String defaultName) {
        for (FieldHandler fh : REGISTERED_FIELD_HANDLERS) {
            if (fh.supportsMethod(method.getAnnotated(), defaultName) && fh.nameForGetterMethod(defaultName) != null) {
                return fh.nameForGetterMethod(defaultName);
            }
        }
        return super.nameForGetterMethod(config, method, defaultName);
    }

    @SuppressWarnings("unchecked")
    @Override
    public String nameForSetterMethod(MapperConfig<?> config, AnnotatedMethod method, String defaultName) {
        for (FieldHandler fh : REGISTERED_FIELD_HANDLERS) {
            if (fh.supportsMethod(method.getAnnotated(), defaultName) && fh.nameForGetterMethod(defaultName) != null) {
                return fh.nameForGetterMethod(defaultName);
            }
        }
        return super.nameForGetterMethod(config, method, defaultName);
    }

}
