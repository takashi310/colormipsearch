package org.janelia.colormipsearch.dao.mongo.support;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

interface FieldHandler {
    boolean supportsField(Field f, String defaultName);
    boolean supportsMethod(Method m, String defaultName);
    String nameForField(String defaultName);
    String nameForGetterMethod(String defaultName);
    String nameForSetterMethod(String defaultName);
}
