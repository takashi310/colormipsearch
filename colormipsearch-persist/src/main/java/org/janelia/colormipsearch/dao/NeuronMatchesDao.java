package org.janelia.colormipsearch.dao;

import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public interface NeuronMatchesDao<M extends AbstractNeuronMetadata,
                                  T extends AbstractNeuronMetadata,
                                  R extends AbstractMatch<M, T>> extends Dao<R> {

    long countNeuronMatches(NeuronSelector maskSelector, NeuronSelector targetSelector);
    PagedResult<R> findNeuronMatches(NeuronSelector maskSelector, NeuronSelector targetSelector, PagedRequest pageRequest);
}
