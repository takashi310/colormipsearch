package org.janelia.colormipsearch.dao;

import java.lang.reflect.ParameterizedType;

import org.janelia.colormipsearch.model.BaseEntity;

/**
 * Abstract base interface for data access.
 *
 * @param <T> entity type
 */
public abstract class AbstractDao<T extends BaseEntity> implements Dao<T> {
    protected Class<T> getEntityType() {
        return getGenericParameterType(this.getClass());
    }

    @SuppressWarnings("unchecked")
    private Class<T> getGenericParameterType(Class<?> parameterizedClass) {
        return (Class<T>)((ParameterizedType)parameterizedClass.getGenericSuperclass()).getActualTypeArguments()[0];
    }
}
