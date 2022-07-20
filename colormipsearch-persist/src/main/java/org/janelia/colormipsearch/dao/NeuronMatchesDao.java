package org.janelia.colormipsearch.dao;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public interface NeuronMatchesDao<M extends AbstractNeuronMetadata,
                                  T extends AbstractNeuronMetadata,
                                  R extends AbstractMatch<M, T>> extends Dao<R> {

    long countNeuronMatches(NeuronSelector maskSelector, NeuronSelector targetSelector);
    PagedResult<R> findNeuronMatches(NeuronSelector maskSelector, NeuronSelector targetSelector, PagedRequest pageRequest);
    // if a match exists it will update it otherwise it will create it.
    void saveOrUpdateAll(List<R> matches, List<Function<R, Pair<String, ?>>> fieldsToUpdateSelectors);
}
