package org.janelia.colormipsearch.dao.mongo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.janelia.colormipsearch.dao.EntityFieldValueHandler;
import org.janelia.colormipsearch.dao.SortCriteria;
import org.janelia.colormipsearch.dao.SortDirection;
import org.janelia.colormipsearch.dao.support.AppendFieldValueHandler;
import org.janelia.colormipsearch.dao.support.IncFieldValueHandler;
import org.janelia.colormipsearch.dao.support.SetOnCreateValueHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MongoDaoHelper {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDaoHelper.class);
    private static final String RECORDS_COUNT_FIELD = "recordsCount";

    static <T, R> List<R> aggregateAsList(List<Bson> aggregationOperators, Bson sortCriteria, long offset, int length,
                                          MongoCollection<T> mongoCollection, Class<R> resultType) {
        List<R> results = new ArrayList<>();
        Iterable<R> resultsItr = aggregateIterable(aggregationOperators, sortCriteria, offset, length, mongoCollection, resultType);
        resultsItr.forEach(results::add);
        return results;
    }

    static <T, R> Iterable<R> aggregateIterable(List<Bson> aggregationOperators, Bson sortCriteria, long offset, int length,
                                                MongoCollection<T> mongoCollection, Class<R> resultType) {
        List<Bson> aggregatePipeline = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(aggregationOperators)) {
            aggregatePipeline.addAll(aggregationOperators);
        }
        if (sortCriteria != null) {
            aggregatePipeline.add(Aggregates.sort(sortCriteria));
        }
        if (offset > 0) {
            aggregatePipeline.add(Aggregates.skip((int) offset));
        }
        if (length > 0) {
            aggregatePipeline.add(Aggregates.limit(length));
        }
        return mongoCollection.aggregate(aggregatePipeline, resultType);
    }

    static <T> Long countAggregate(List<Bson> aggregationOperators, MongoCollection<T> mongoCollection) {
        List<Bson> aggregatePipeline = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(aggregationOperators)) {
            aggregatePipeline.addAll(aggregationOperators);
        }
        aggregatePipeline.add(Aggregates.count(RECORDS_COUNT_FIELD));
        Document recordsCountDoc = mongoCollection.aggregate(aggregatePipeline, Document.class).first();
        if (recordsCountDoc == null) {
            return 0L;
        } else if (recordsCountDoc.get(RECORDS_COUNT_FIELD) instanceof Integer) {
            return recordsCountDoc.getInteger(RECORDS_COUNT_FIELD).longValue();
        } else if (recordsCountDoc.get(RECORDS_COUNT_FIELD) instanceof Long) {
            return recordsCountDoc.getLong(RECORDS_COUNT_FIELD);
        } else {
            LOG.error("Unknown records count field type: {}", recordsCountDoc);
            throw new IllegalStateException("Unknown RECORDS COUNT FIELD TYPE " + recordsCountDoc);
        }
    }

    static Bson createBsonFilterCriteria(List<Bson> filters) {
        if (CollectionUtils.isNotEmpty(filters)) {
            return filters.stream()
                    .filter(Objects::nonNull)
                    .reduce(Filters::and)
                    .orElse(new Document());
        } else {
            return new Document();
        }
    }

    static Bson createBsonSortCriteria(List<SortCriteria> sortCriteria) {
        if (CollectionUtils.isNotEmpty(sortCriteria)) {
            Map<String, Object> sortCriteriaAsMap = sortCriteria.stream()
                    .filter(sc -> StringUtils.isNotBlank(sc.getField()))
                    // Convert "id" to "_id" if necessary
                    .map(sc -> new SortCriteria("id".equals(sc.getField()) ? "_id" : sc.getField(), sc.getDirection()))
                    .collect(Collectors.toMap(
                            SortCriteria::getField,
                            sc -> sc.getDirection() == SortDirection.DESC ? -1 : 1,
                            (sc1, sc2) -> sc2,
                            LinkedHashMap::new));
            return new Document(sortCriteriaAsMap);
        } else {
            return null;
        }
    }

    static <T> Bson createAttributeFilter(String attributeName, T attributeValue) {
        return Filters.eq(attributeName, attributeValue);
    }

    static <I> Bson createFilterById(I id) {
        return Filters.eq("_id", id);
    }

    static <I> Bson createFilterByIds(Collection<I> ids) {
        return Filters.in("_id", ids);
    }

    static Bson createFilterByClass(Class<?> clazz) {
        return clazz != null ? Filters.eq("class", clazz.getName()) : new Document();
    }

    static Bson createFilterByClass(String clazz) {
        return StringUtils.isNotBlank(clazz) ? Filters.eq("class", clazz) : new Document();
    }

    static <I, T, R> R findById(I id, MongoCollection<T> mongoCollection, Class<R> documentType) {
        if (id == null) {
            return null;
        } else {
            List<R> entityDocs = find(
                    createFilterById(id),
                    null,
                    0,
                    2,
                    mongoCollection,
                    documentType
            );
            return CollectionUtils.isNotEmpty(entityDocs) ? entityDocs.get(0) : null;
        }
    }

    static <I, T, R> List<R> findByIds(Collection<I> ids, MongoCollection<T> mongoCollection, Class<R> documentType) {
        if (CollectionUtils.isNotEmpty(ids)) {
            return find(createFilterByIds(ids), null, 0, 0, mongoCollection, documentType);
        } else {
            return Collections.emptyList();
        }
    }

    static <T, R> List<R> find(Bson queryFilter, Bson sortCriteria, long offset, int length, MongoCollection<T> mongoCollection, Class<R> resultType) {
        List<R> entityDocs = new ArrayList<>();
        FindIterable<R> results = mongoCollection.find(resultType);
        if (queryFilter != null) {
            results = results.filter(queryFilter);
        }
        if (offset > 0) {
            results = results.skip((int) offset);
        }
        if (length > 0) {
            results = results.limit(length);
        }
        return results
                .sort(sortCriteria)
                .into(entityDocs);
    }

    @SuppressWarnings("unchecked")
    static Bson getFieldUpdate(String fieldName, EntityFieldValueHandler<?> valueHandler) {
        if (valueHandler == null || valueHandler.getFieldValue() == null) {
            return Updates.unset(fieldName);
        } else if (valueHandler instanceof AppendFieldValueHandler) {
            Object value = valueHandler.getFieldValue();
            if (value instanceof Iterable) {
                if (Set.class.isAssignableFrom(value.getClass())) {
                    return Updates.addEachToSet(
                            fieldName,
                            StreamSupport.stream(((Iterable<?>) value).spliterator(), false).collect(Collectors.toList())
                    );
                } else {
                    return Updates.pushEach(
                            fieldName,
                            StreamSupport.stream(((Iterable<?>) value).spliterator(), false).collect(Collectors.toList()));
                }
            } else {
                return Updates.push(fieldName, value);
            }
        } else if (valueHandler instanceof IncFieldValueHandler) {
            return Updates.inc(fieldName, (Number) valueHandler.getFieldValue());
        } else if (valueHandler instanceof SetOnCreateValueHandler) {
            return Updates.setOnInsert(fieldName, valueHandler.getFieldValue());
        } else {
            return Updates.set(fieldName, valueHandler.getFieldValue());
        }
    }

}
