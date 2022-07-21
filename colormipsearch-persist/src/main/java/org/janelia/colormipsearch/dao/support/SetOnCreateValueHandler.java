package org.janelia.colormipsearch.dao.support;

/**
 * Set value only first time when the document is created.
 * @param <T>
 */
public class SetOnCreateValueHandler<T> extends AbstractEntityFieldValueHandler<T> {
    public SetOnCreateValueHandler(T fieldValue) {
        super(fieldValue);
    }
}
