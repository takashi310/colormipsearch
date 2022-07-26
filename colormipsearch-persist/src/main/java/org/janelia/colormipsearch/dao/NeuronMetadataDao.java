package org.janelia.colormipsearch.dao;

import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.datarequests.PagedResult;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public interface NeuronMetadataDao<N extends AbstractNeuronMetadata> extends Dao<N> {
    /**
     * Create or update neuron. If a neuron exists, update it otherwise create it.
     * @param neuron to create or update.
     */
    void createOrUpdate(N neuron);

    /**
     * Search neurons that match the provided selection criteria.
     *
     * @param neuronSelector neuron selection criteria
     * @param pageRequest pagination parameters
     * @return
     */
    PagedResult<N> findNeurons(NeuronSelector neuronSelector, PagedRequest pageRequest);
}
