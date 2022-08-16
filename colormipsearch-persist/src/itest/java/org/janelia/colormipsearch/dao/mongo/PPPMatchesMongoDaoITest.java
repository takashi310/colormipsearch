package org.janelia.colormipsearch.dao.mongo;

import java.util.ArrayList;
import java.util.List;

import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.EMNeuronEntity;
import org.janelia.colormipsearch.model.LMNeuronEntity;
import org.janelia.colormipsearch.model.PPPMatchEntity;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class PPPMatchesMongoDaoITest extends AbstractMongoDaoITest {

    private final List<? extends PPPMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> testData = new ArrayList<>();

    @After
    public <M extends AbstractNeuronEntity,
            T extends AbstractNeuronEntity>
    void tearDown() {
        // delete the data that was created for testing
        NeuronMatchesDao<PPPMatchEntity<M, T>> neuronMatchesDao = daosProvider.getPPPMatchesDao();
        @SuppressWarnings("unchecked")
        List<PPPMatchEntity<M, T>> toDelete = (List<PPPMatchEntity<M, T>>) testData;
        deleteAll(neuronMatchesDao, toDelete);
    }

    @Test
    public void persistPPPMatchTestData() {
        PPPMatchEntity<EMNeuronEntity, LMNeuronEntity> testPPPMatch =
                createTestPPPMatch(
                        new TestNeuronEntityBuilder<>(EMNeuronEntity::new)
                                .entityId(10)
                                .get(),
                        new TestNeuronEntityBuilder<>(LMNeuronEntity::new)
                                .entityId(20)
                                .get(),
                        0.5
                );
        NeuronMatchesDao<PPPMatchEntity<EMNeuronEntity, LMNeuronEntity>> neuronMatchesDao =
                daosProvider.getPPPMatchesDao();
        neuronMatchesDao.save(testPPPMatch);
        PPPMatchEntity<EMNeuronEntity, LMNeuronEntity> persistedPPPMatch = neuronMatchesDao.findByEntityId(testPPPMatch.getEntityId());
        assertNotNull(persistedPPPMatch);
        assertNotSame(testPPPMatch, persistedPPPMatch);
        assertEquals(testPPPMatch.getEntityId(), persistedPPPMatch.getEntityId());
        assertNotNull(persistedPPPMatch.getMaskImage());
        assertSame(persistedPPPMatch.getMaskImage(), persistedPPPMatch.getSourceImage());
        assertNotNull(persistedPPPMatch.getMatchedImage());
        assertSame(persistedPPPMatch.getMatchedImage(), persistedPPPMatch.getTargetImage());
        assertEquals(testPPPMatch.getMaskImageRefId().toString(), persistedPPPMatch.getMaskImageRefId().toString());
        assertEquals(testPPPMatch.getMatchedImageRefId().toString(), persistedPPPMatch.getMatchedImageRefId().toString());
    }

    private <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity>
    PPPMatchEntity<M, T> createTestPPPMatch(M maskNeuron, T targetNeuron, double rank) {
        PPPMatchEntity<M, T> testMatch = new PPPMatchEntity<>();
        testMatch.setSourceEmName("sourceEm");
        testMatch.setMaskImage(maskNeuron);
        testMatch.setSourceLmName("sourceLm");
        testMatch.setMatchedImage(targetNeuron);
        testMatch.setRank(rank);
        addTestData(testMatch);
        return testMatch;
    }

    @SuppressWarnings("unchecked")
    private <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity, R extends PPPMatchEntity<M, T>> void addTestData(R o) {
        ((List<PPPMatchEntity<M, T>>) testData).add(o);
    }
}
