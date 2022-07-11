package org.janelia.colormipsearch.dao.mongo;

import org.janelia.colormipsearch.dao.NeuronMatchDao;
import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDSMatch;
import org.janelia.colormipsearch.model.EMNeuronMetadata;
import org.janelia.colormipsearch.model.LMNeuronMetadata;
import org.junit.Before;
import org.junit.Test;

public class NeuronMetadataMongoDaoITest extends AbstractMongoDaoITest {

    private NeuronMetadataDao<AbstractNeuronMetadata> testDao;

    @Before
    public void setUp() {
        testDao = new NeuronMetadataMongoDao<>(testMongoDatabase, idGenerator);
    }

    @Test
    public void persistEmNeuron() {
        EMNeuronMetadata testEmNeuron = new EMNeuronMetadata();
        testDao.save(testEmNeuron);
        System.out.println("!!!! DONE");
    }
}
