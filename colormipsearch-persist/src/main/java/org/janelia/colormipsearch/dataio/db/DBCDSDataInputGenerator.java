package org.janelia.colormipsearch.dataio.db;

import org.janelia.colormipsearch.config.Config;
import org.janelia.colormipsearch.dao.DaosProvider;
import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dataio.CDSDataInputGenerator;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public class DBCDSDataInputGenerator implements CDSDataInputGenerator {

    private final NeuronMetadataDao<AbstractNeuronMetadata> neuronMetadataDao;

    public DBCDSDataInputGenerator(Config config) {
        this.neuronMetadataDao = DaosProvider.getInstance(config).getNeuronMetadataDao();
    }

    @Override
    public void write(AbstractNeuronMetadata neuronMetadata) {
        neuronMetadataDao.save(neuronMetadata);
    }

    @Override
    public void done() {
    }
}
