package org.janelia.colormipsearch.dao;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public interface NeuronMatchesDao<M extends AbstractNeuronMetadata,
                                  T extends AbstractNeuronMetadata,
                                  R extends AbstractMatch<M, T>> extends Dao<R> {
    /**
     * Count neuron matches filtered by the type and scores specified by neuronsMatchFilter and/or by the specified
     * mask and target selectors.
     *
     * @param neuronsMatchFilter matches score filter
     * @param maskSelector filter by mask attributes
     * @param targetSelector filter by target attributes
     * @return
     */
    long countNeuronMatches(NeuronsMatchFilter<R> neuronsMatchFilter,
                            NeuronSelector maskSelector,
                            NeuronSelector targetSelector);

    /**
     * Retrieve neuron matches filtered by the type and scores specified by neuronsMatchFilter and/or by the specified
     * mask and target selectors.
     *
     * @param neuronsMatchFilter matches score filter
     * @param maskSelector filter by mask attributes
     * @param targetSelector filter by target attributes
     * @param pageRequest pagination parameters
     * @return
     */
    PagedResult<R> findNeuronMatches(NeuronsMatchFilter<R> neuronsMatchFilter,
                                     NeuronSelector maskSelector,
                                     NeuronSelector targetSelector,
                                     PagedRequest pageRequest);
    // if a match exists it will update it otherwise it will create it.
    void saveOrUpdateAll(List<R> matches, List<Function<R, Pair<String, ?>>> fieldsToUpdateSelectors);
}
