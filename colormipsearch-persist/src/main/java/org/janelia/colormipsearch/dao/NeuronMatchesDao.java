package org.janelia.colormipsearch.dao;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.datarequests.PagedResult;
import org.janelia.colormipsearch.model.AbstractMatchEntity;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;

public interface NeuronMatchesDao<R extends AbstractMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> extends Dao<R> {
    class NeuronField<V> {
        private final String fieldName;
        private final boolean toBeAppended;
        private final V value;

        public NeuronField(String fieldName, boolean toBeAppended, V value) {
            this.fieldName = fieldName;
            this.toBeAppended = toBeAppended;
            this.value = value;
        }

        public String getFieldName() {
            return fieldName;
        }

        public boolean isToBeAppended() {
            return toBeAppended;
        }

        public V getValue() {
            return value;
        }
    }

    /**
     * Create or update matches. If a matcb, for the given mask entity ID and target entity ID, exists then update the fields
     * otherwise create it.
     * @param matches to create or update
     * @param fieldsToUpdateSelectors fields to update if the record exists
     */
    void createOrUpdateAll(List<R> matches, List<Function<R, NeuronField<?>>> fieldsToUpdateSelectors);

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

    /**
     * Update existing matches. The match must have a valid id otherwise an exceptio is thrown.
     * @param matches to create or update
     * @param fieldsToUpdateSelectors fields to update if the record exists
     * @throws IllegalArgumentException if any of the matches from the list does not have a valid entity ID.
     */
    void updateExistingMatches(List<R> matches, List<Function<R, Pair<String, ?>>> fieldsToUpdateSelectors);
}
