package org.janelia.colormipsearch.dao;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.janelia.colormipsearch.model.BaseEntity;

/**
 * Base interface for data access.
 *
 * @param <T> entity type
 */
public interface Dao<T extends BaseEntity> {
    // Read operations
    T findByEntityId(Number id);
    List<T> findByEntityIds(Collection<Number> ids);
    PagedResult<T> findAll(PagedRequest pageRequest);
    long countAll();
    // Write operations
    void save(T entity);
    void saveAll(List<T> entities);
    T update(Number entityId, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate);
    void delete(T entity);
}
