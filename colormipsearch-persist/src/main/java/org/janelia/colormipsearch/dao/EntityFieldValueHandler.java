package org.janelia.colormipsearch.dao;

/**
 * @param <T> field value type
 */
public interface EntityFieldValueHandler<T> {
    T getFieldValue();
}
