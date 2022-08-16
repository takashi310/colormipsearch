package org.janelia.colormipsearch.dao.mongo.support;

import com.fasterxml.jackson.annotation.JsonFilter;

import org.janelia.colormipsearch.model.AbstractBaseEntity;
import org.janelia.colormipsearch.model.AbstractMatchEntity;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;

/**
 * The purpose of this MixIn is just to use JsonFilter annotation
 * and it is only used when serializing to Mongo.
 * So far when we write JSON to the file system we don't want to use any filter.
 * @param <T> entity type
 */
@JsonFilter(MongoIgnoredFieldFilter.FILTER_NAME)
public abstract class ApplyMongoIgnoreFilterMixIn<T> extends AbstractBaseEntity {
}
