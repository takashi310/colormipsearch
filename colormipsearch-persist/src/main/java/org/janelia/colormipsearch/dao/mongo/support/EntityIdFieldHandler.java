package org.janelia.colormipsearch.dao.mongo.support;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.janelia.colormipsearch.model.annotations.EntityId;

class EntityIdFieldHandler implements FieldHandler {

    @Override
    public boolean supportsField(Field f, String defaultName) {
        return f.isAnnotationPresent(EntityId.class);
    }

    @Override
    public boolean supportsMethod(Method m, String defaultName) {
        return "entityId".equals(defaultName);
    }

    @Override
    public String nameForField(String defaultName) {
        return "_id";
    }

    @Override
    public String nameForGetterMethod(String defaultName) {
        return "_id";
    }

    @Override
    public String nameForSetterMethod(String defaultName) {
        return "_id";
    }
}
