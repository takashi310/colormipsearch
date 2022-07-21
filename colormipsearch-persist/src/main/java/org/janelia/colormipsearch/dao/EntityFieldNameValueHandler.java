package org.janelia.colormipsearch.dao;

/**
 * @param <T> field value type
 */
public class EntityFieldNameValueHandler<T> {
    private final String fieldName;
    private final EntityFieldValueHandler<T> valueHandler;

    public EntityFieldNameValueHandler(String fieldName, EntityFieldValueHandler<T> valueHandler) {
        this.fieldName = fieldName;
        this.valueHandler = valueHandler;
    }

    public String getFieldName() {
        return fieldName;
    }

    public EntityFieldValueHandler<T> getValueHandler() {
        return valueHandler;
    }
}
