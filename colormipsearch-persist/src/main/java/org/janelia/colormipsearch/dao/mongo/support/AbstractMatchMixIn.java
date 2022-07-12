package org.janelia.colormipsearch.dao.mongo.support;

import com.fasterxml.jackson.annotation.JsonFilter;

import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

@JsonFilter("useRefIdFilter")
public abstract class AbstractMatchMixIn<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> extends AbstractMatch<M, T> {
}
