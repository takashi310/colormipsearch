package org.janelia.colormipsearch.dao;

import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.datarequests.PagedResult;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public interface NeuronMetadataDao<N extends AbstractNeuronMetadata> extends Dao<N> {
    /**
     * Search neurons that match the provided selection criteria.
     *
     * @param neuronSelector neuron selection criteria
     * @param pageRequest pagination parameters
     * @return
     */
    PagedResult<N> findNeurons(NeuronSelector neuronSelector, PagedRequest pageRequest);
}
