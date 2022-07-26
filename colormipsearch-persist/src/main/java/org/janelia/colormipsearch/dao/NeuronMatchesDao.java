package org.janelia.colormipsearch.dao;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.datarequests.PagedResult;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;

public interface NeuronMatchesDao<R extends AbstractMatch<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> extends Dao<R> {
    /**
     * Create or update matches. If a matcb, for the given mask entity ID and target entity ID, exists then update the fields
     * otherwise create it.
     * @param matches to create or update
     * @param fieldsToUpdateSelectors fields to update if the record exists
     */
    void createOrUpdateAll(List<R> matches, List<Function<R, Pair<String, ?>>> fieldsToUpdateSelectors);

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
     * @param neuronsMatchFilter score filter as well as mask and target entity IDs
     * @param maskSelector filter by mask attributes
     * @param targetSelector filter by target attributes
     * @param pageRequest pagination parameters
     * @return
     */
    PagedResult<R> findNeuronMatches(NeuronsMatchFilter<R> neuronsMatchFilter,
                                     NeuronSelector maskSelector,
                                     NeuronSelector targetSelector,
                                     PagedRequest pageRequest);
}
