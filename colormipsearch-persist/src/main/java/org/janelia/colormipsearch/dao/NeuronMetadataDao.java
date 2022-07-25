package org.janelia.colormipsearch.dao;

import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.datarequests.PagedResult;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public interface NeuronMetadataDao<N extends AbstractNeuronMetadata> extends Dao<N> {
    PagedResult<N> findNeurons(NeuronSelector neuronSelector, PagedRequest pageRequest);
}
