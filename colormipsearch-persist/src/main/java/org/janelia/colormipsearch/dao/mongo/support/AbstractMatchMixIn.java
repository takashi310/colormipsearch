package org.janelia.colormipsearch.dao.mongo.support;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

/**
 * The purpose of this MixIn is just to use JsonFilter annotation
 * and it is only used when serializing to Mongo.
 * So far when we write JSON to the file system we don't want to use any filter.
 * @param <M> mask image type
 * @param <T> target image type
 */
@JsonFilter("useRefIdFilter")
public abstract class AbstractMatchMixIn<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> extends AbstractMatch<M, T> {
}
