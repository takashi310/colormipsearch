package org.janelia.colormipsearch.dao.support;

/**
 * @param <T> field value type
 */
public class IncFieldValueHandler<T extends Number> extends AbstractEntityFieldValueHandler<T> {
    public IncFieldValueHandler(T fieldValue) {
        super(fieldValue);
    }
}
