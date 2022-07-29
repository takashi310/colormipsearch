package org.janelia.colormipsearch.dao;

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
}
