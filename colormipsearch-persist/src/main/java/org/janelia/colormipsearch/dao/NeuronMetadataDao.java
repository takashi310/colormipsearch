package org.janelia.colormipsearch.dao;

import java.util.List;

import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.datarequests.PagedResult;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;

public interface NeuronMetadataDao<N extends AbstractNeuronEntity> extends Dao<N> {
    /**
     * Create or update neuron. If a neuron exists, update it otherwise create it.
     * @param neuron to create or update.
     */
    N createOrUpdate(N neuron);

    /**
     * Search neurons that match the provided selection criteria.
     *
     * @param neuronSelector neuron selection criteria
     * @param pageRequest pagination parameters
     * @return
     */
    PagedResult<N> findNeurons(NeuronSelector neuronSelector, PagedRequest pageRequest);

    /**
     * Find all distinct values for the specified attribute using the given neuron selector for filtering.
     *
     * @param attributeName neuron attribute values projected in the result
     * @param neuronSelector neuron filter
     * @return
     */
    List<String> findAllNeuronAttributeValues(String attributeName, NeuronSelector neuronSelector);
}
