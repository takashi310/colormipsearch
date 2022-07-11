package org.janelia.colormipsearch.dao.mongo;

import java.util.ArrayList;
import java.util.List;

import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.model.EMNeuronMetadata;
import org.janelia.colormipsearch.model.LMNeuronMetadata;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public class NeuronMatchesMongoDaoITest extends AbstractMongoDaoITest {

    private final List<? extends AbstractMatch<? extends AbstractNeuronMetadata, ? extends AbstractNeuronMetadata>> testData = new ArrayList<>();

    @After
    public <M extends AbstractNeuronMetadata,
            T extends AbstractNeuronMetadata>
    void tearDown() {
        // delete the data that was created for testing
        NeuronMatchesDao<M, T, AbstractMatch<M, T>> testDao = createTestDao();
        @SuppressWarnings("unchecked")
        List<AbstractMatch<M, T>> toDelete = (List<AbstractMatch<M, T>>) testData;
        deleteAll(testDao, toDelete);
    }

    private <M extends AbstractNeuronMetadata,
             T extends AbstractNeuronMetadata,
             R extends AbstractMatch<M, T>>
    NeuronMatchesDao<M, T, R> createTestDao() {
        return new NeuronMatchesMongoDao<>(testMongoDatabase, idGenerator);
    }

    @Test
    public void persistTestData() {
        CDMatch<EMNeuronMetadata, LMNeuronMetadata> testCDMatch = createTestCDMatch(new EMNeuronMetadata(), new LMNeuronMetadata());
        NeuronMatchesDao<EMNeuronMetadata, LMNeuronMetadata, CDMatch<EMNeuronMetadata, LMNeuronMetadata>> testDao = createTestDao();
        testDao.save(testCDMatch);
        CDMatch<EMNeuronMetadata, LMNeuronMetadata> persistedCDMatch = testDao.findByEntityId(testCDMatch.getEntityId());
        assertNotNull(persistedCDMatch);
        assertNotSame(testCDMatch, persistedCDMatch);
        assertEquals(testCDMatch, persistedCDMatch);
    }

    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata>
    CDMatch<M, T> createTestCDMatch(M maskNeuron, T targetNeuron) {
        CDMatch<M, T> testMatch = new CDMatch<>();
        testMatch.setMaskImage(maskNeuron);
        testMatch.setMatchedImage(targetNeuron);
        addTestData(testMatch);
        return testMatch;
    }

    @SuppressWarnings("unchecked")
    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata,R extends AbstractMatch<M, T>> void addTestData(R o) {
        List<AbstractMatch<M, T>> addTo = (List<AbstractMatch<M, T>>) testData;
        addTo.add(o);
    }
}
