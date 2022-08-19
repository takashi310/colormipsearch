package org.janelia.colormipsearch.dataio.db;

import java.util.List;

import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dataio.CDMIPsWriter;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;

public class DBCDMIPsWriter implements CDMIPsWriter {

    private final NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao;

    public DBCDMIPsWriter(NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao) {
        this.neuronMetadataDao = neuronMetadataDao;
    }

    @Override
    public void open() {
        // nothing to do for the DB writer
    }

    @Override
    public void write(List<AbstractNeuronEntity> neuronMetadata) {
        neuronMetadataDao.saveAll(neuronMetadata);
    }

    @Override
    public void close() {
        // nothing to do for the DB writer
    }
}
