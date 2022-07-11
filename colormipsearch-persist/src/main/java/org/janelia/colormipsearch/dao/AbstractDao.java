package org.janelia.colormipsearch.dao;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.TypeVariable;

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
        Object typeArg = ((ParameterizedType)parameterizedClass.getGenericSuperclass()).getActualTypeArguments()[0];
        if (typeArg instanceof TypeVariable) {
            Object t =  ((TypeVariable<?>) typeArg).getBounds()[0];
            if (t instanceof ParameterizedType) {
                String className = ((ParameterizedType) t).getRawType().getTypeName();
                try {
                    return (Class<T>) Class.forName(className);
                } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException(e);
                }
            } else {
                return (Class<T>) t;
            }
        } else {
            return (Class<T>) typeArg;
        }
    }
}
