package org.janelia.colormipsearch.dataio.db;

import java.util.List;

import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dataio.CDMIPsWriter;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;

public class DBCheckedCDMIPsWriter implements CDMIPsWriter {

    private final NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao;

    public DBCheckedCDMIPsWriter(NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao) {
        this.neuronMetadataDao = neuronMetadataDao;
    }

    @Override
    public void open() {
        // nothing to do for the DB writer
    }

    @Override
    public void write(List<AbstractNeuronEntity> neuronMetadata) {
        neuronMetadata.forEach(neuronMetadataDao::createOrUpdate);
    }

    @Override
    public void writeOne(AbstractNeuronEntity neuronMetadata) {
        neuronMetadataDao.createOrUpdate(neuronMetadata);
    }

    @Override
    public void close() {
        // nothing to do for the DB writer
    }
}
