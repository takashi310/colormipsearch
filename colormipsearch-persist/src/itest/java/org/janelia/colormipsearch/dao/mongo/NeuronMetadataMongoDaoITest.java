package org.janelia.colormipsearch.dao.mongo;

import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.EMNeuronMetadata;
import org.junit.Test;

public class NeuronMetadataMongoDaoITest extends AbstractMongoDaoITest {


    public <N extends AbstractNeuronMetadata> NeuronMetadataDao<N> createTestDao(Class<N> neuronMetadataClass) {
        return new NeuronMetadataMongoDao<>(testMongoDatabase, idGenerator);
    }

    @Test
    public void persistEmNeuron() {
        NeuronMetadataDao<EMNeuronMetadata> testDao = createTestDao(EMNeuronMetadata.class);

        EMNeuronMetadata testEmNeuron = new EMNeuronMetadata();
        testDao.save(testEmNeuron);
        System.out.println("!!!! DONE");
    }
}
