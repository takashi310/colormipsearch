package org.janelia.colormipsearch.dataio.db;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dataio.CDMIPsWriter;
import org.janelia.colormipsearch.model.AbstractBaseEntity;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.ProcessingType;

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
    public void write(List<? extends AbstractNeuronEntity> neuronEntities) {
        neuronEntities.forEach(neuronMetadataDao::createOrUpdate);
    }

    @Override
    public void writeOne(AbstractNeuronEntity neuronEntity) {
        neuronMetadataDao.createOrUpdate(neuronEntity);
    }

    @Override
    public void addProcessingTags(List<? extends AbstractNeuronEntity> neuronEntities, ProcessingType processingType, Set<String> tags) {
        neuronMetadataDao.addProcessingTags(
                neuronEntities.stream().map(AbstractBaseEntity::getEntityId).collect(Collectors.toList()),
                processingType,
                tags);
    }

    @Override
    public void close() {
        // nothing to do for the DB writer
    }
}
