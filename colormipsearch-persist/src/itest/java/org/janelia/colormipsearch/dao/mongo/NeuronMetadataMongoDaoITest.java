package org.janelia.colormipsearch.dao.mongo;

import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.EMNeuronMetadata;
import org.junit.Test;

public class NeuronMetadataMongoDaoITest extends AbstractMongoDaoITest {

    @Test
    public void persistEmNeuron() {
        NeuronMetadataDao<EMNeuronMetadata> testDao = TestDBUtils.createNeuronMetadataDao(testMongoDatabase, idGenerator);

        EMNeuronMetadata testEmNeuron = new EMNeuronMetadata();
        testDao.save(testEmNeuron);
        System.out.println("!!!! DONE");
    }
}
