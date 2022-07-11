package org.janelia.colormipsearch.dao.mongo;

import org.janelia.colormipsearch.dao.NeuronMatchDao;
import org.janelia.colormipsearch.model.CDSMatch;
import org.janelia.colormipsearch.model.EMNeuronMetadata;
import org.janelia.colormipsearch.model.LMNeuronMetadata;
import org.junit.Before;
import org.junit.Test;

public class NeuronMatchMongoDaoITest extends AbstractMongoDaoITest {

    private NeuronMatchDao<EMNeuronMetadata, LMNeuronMetadata, CDSMatch<EMNeuronMetadata, LMNeuronMetadata>> testDao;

    @Before
    public void setUp() {
        testDao = new NeuronMatchMongoDao<>(testMongoDatabase, idGenerator);
    }

    @Test
    public void persistTestData() {
        CDSMatch<EMNeuronMetadata, LMNeuronMetadata> testMatch = new CDSMatch<>();
        testDao.save(testMatch);
        System.out.println("!!!! DONE");
    }
}
