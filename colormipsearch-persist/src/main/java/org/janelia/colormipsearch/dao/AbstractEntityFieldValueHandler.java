package org.janelia.colormipsearch.dao;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * @param <T> field value type
 */
public abstract class AbstractEntityFieldValueHandler<T> implements EntityFieldValueHandler<T> {
    private final T fieldValue;

    public AbstractEntityFieldValueHandler(T fieldValue) {
        this.fieldValue = fieldValue;
    }

    @Override
    public T getFieldValue() {
        return fieldValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        AbstractEntityFieldValueHandler<?> that = (AbstractEntityFieldValueHandler<?>) o;

        return new EqualsBuilder()
                .append(getFieldValue(), that.getFieldValue())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(fieldValue)
                .toHashCode();
    }
}
