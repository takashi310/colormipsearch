package org.janelia.colormipsearch.dao.mongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.janelia.colormipsearch.dao.AbstractDao;
import org.janelia.colormipsearch.dao.Dao;
import org.janelia.colormipsearch.dao.EntityFieldValueHandler;
import org.janelia.colormipsearch.dao.PagedRequest;
import org.janelia.colormipsearch.dao.PagedResult;
import org.janelia.colormipsearch.dao.support.AppendFieldValueHandler;
import org.janelia.colormipsearch.dao.support.EntityUtils;
import org.janelia.colormipsearch.dao.support.IdGenerator;
import org.janelia.colormipsearch.dao.support.IncFieldValueHandler;
import org.janelia.colormipsearch.model.BaseEntity;
import org.janelia.colormipsearch.model.annotations.PersistenceInfo;

/**
 * Abstract Mongo DAO.
 *
 * @param <T> entity type
 */
public abstract class AbstractMongoDao<T extends BaseEntity> extends AbstractDao<T> implements Dao<T> {

    protected final IdGenerator idGenerator;
    final MongoCollection<T> mongoCollection;

    AbstractMongoDao(MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        this.idGenerator = idGenerator;
        mongoCollection = mongoDatabase.getCollection(getEntityCollectionName(), getEntityType());
    }

    private String getEntityCollectionName() {
        Class<T> entityClass = getEntityType();
        PersistenceInfo persistenceInfo = EntityUtils.getPersistenceInfo(entityClass);
        if (persistenceInfo == null) {
            throw new IllegalArgumentException("Entity class " + entityClass.getName() + " is not annotated with MongoMapping");
        }
        return persistenceInfo.storeName();
    }

    /**
     * This is a placeholder for creating the collection indexes. For now nobody invokes this method.
     */
    abstract protected void createDocumentIndexes();

    @Override
    public T findByEntityId(Number id) {
        return MongoDaoHelper.findById(id, mongoCollection, getEntityType());
    }

    @Override
    public List<T> findByEntityIds(Collection<Number> ids) {
        return MongoDaoHelper.findByIds(ids, mongoCollection, getEntityType());
    }

    @Override
    public PagedResult<T> findAll(Class<T> type, PagedRequest pageRequest) {
        return new PagedResult<>(pageRequest,
                MongoDaoHelper.find(
                        MongoDaoHelper.createFilterByClass(type),
                        MongoDaoHelper.createBsonSortCriteria(pageRequest.getSortCriteria()),
                        pageRequest.getOffset(),
                        pageRequest.getPageSize(),
                        mongoCollection,
                        getEntityType()
                )
        );
    }

    @Override
    public long countAll() {
        return mongoCollection.countDocuments();
    }

    @Override
    public void save(T entity) {
        if (!entity.hasEntityId()) {
            entity.setEntityId(idGenerator.generateId());
            entity.setCreatedDate(new Date());
            try {
                mongoCollection.insertOne(entity);
            } catch (MongoWriteException e) {
                if (e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
                    throw new IllegalArgumentException(e);
                }
            }
        }
    }

    public void saveAll(List<T> entities) {
        Iterator<Number> idIterator = idGenerator.generateIdList(entities.size()).iterator();
        List<T> toInsert = new ArrayList<>();
        Date currentDate = new Date();
        entities.forEach(e -> {
            if (!e.hasEntityId()) {
                e.setEntityId(idIterator.next());
                e.setCreatedDate(currentDate);
                toInsert.add(e);
            }
        });
        if (!toInsert.isEmpty()) {
            try {
                mongoCollection.insertMany(toInsert);
            } catch (MongoWriteException e) {
                if (e.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
                    throw new IllegalArgumentException(e);
                }
            }
        }
    }

    @Override
    public T update(Number entityId, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        FindOneAndUpdateOptions updateOptions = new FindOneAndUpdateOptions();
        updateOptions.returnDocument(ReturnDocument.AFTER);
        updateOptions.upsert(false);

        if (fieldsToUpdate.isEmpty()) {
            return findByEntityId(entityId);
        } else {
            return mongoCollection.findOneAndUpdate(
                    getIdMatchFilter(entityId),
                    getUpdates(fieldsToUpdate),
                    updateOptions
            );
        }
    }

    protected Bson getUpdates(Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        List<Bson> fieldUpdates = fieldsToUpdate.entrySet().stream()
                .map(e -> MongoDaoHelper.getFieldUpdate(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        return Updates.combine(fieldUpdates);
    }

    private Bson getIdMatchFilter(Number id) {
        return Filters.eq("_id", id);
    }

    @Override
    public void delete(T entity) {
        mongoCollection.deleteOne(getIdMatchFilter(entity.getEntityId()));
    }

}
