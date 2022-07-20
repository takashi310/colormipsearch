package org.janelia.colormipsearch.dao.mongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.janelia.colormipsearch.dao.SortCriteria;
import org.janelia.colormipsearch.dao.SortDirection;

class MongoDaoHelper {

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

    static <I> Bson createFilterById(I id) {
        return Filters.eq("_id", id);
    }

    static <I> Bson createFilterByIds(Collection<I> ids) {
        return Filters.in("_id", ids);
    }

    static Bson createFilterByClass(Class<?> clazz) {
        return Filters.eq("class", clazz.getName());
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

}
