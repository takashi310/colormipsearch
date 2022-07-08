package org.janelia.colormipsearch.dao.support;

/**
 * @param <T> field value type
 */
public class AppendFieldValueHandler<T> extends AbstractEntityFieldValueHandler<T> {
    public AppendFieldValueHandler(T fieldValue) {
        super(fieldValue);
    }
}
