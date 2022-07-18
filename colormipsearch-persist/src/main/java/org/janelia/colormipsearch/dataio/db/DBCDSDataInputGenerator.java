package org.janelia.colormipsearch.dataio.db;

import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dataio.CDSDataInputGenerator;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public class DBCDSDataInputGenerator implements CDSDataInputGenerator {

    private NeuronMetadataDao<AbstractNeuronMetadata> neuronMetadataDao;

    @Override
    public void write(AbstractNeuronMetadata neuronMetadata) {
        neuronMetadataDao.save(neuronMetadata);
    }

    @Override
    public void done() {
    }
}
