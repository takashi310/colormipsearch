package org.janelia.colormipsearch.dao;

import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public interface NeuronMatchDao<M extends AbstractNeuronMetadata,
                                T extends AbstractNeuronMetadata,
                                R extends AbstractMatch<M, T>> extends Dao<R> {
}
