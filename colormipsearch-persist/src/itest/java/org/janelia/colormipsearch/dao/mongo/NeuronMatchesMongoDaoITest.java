package org.janelia.colormipsearch.dao.mongo;

import java.util.ArrayList;
import java.util.List;

import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.model.EMNeuronMetadata;
import org.janelia.colormipsearch.model.LMNeuronMetadata;
import org.janelia.colormipsearch.model.PPPMatch;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public class NeuronMatchesMongoDaoITest extends AbstractMongoDaoITest {

    private final List<? extends AbstractMatch<? extends AbstractNeuronMetadata, ? extends AbstractNeuronMetadata>> testData = new ArrayList<>();

    @After
    public <M extends AbstractNeuronMetadata,
            T extends AbstractNeuronMetadata>
    void tearDown() {
        // delete the data that was created for testing
        NeuronMatchesDao<M, T, AbstractMatch<M, T>> testDao =
                TestDBUtils.createNeuronMatchesDao(testMongoDatabase, idGenerator);
        @SuppressWarnings("unchecked")
        List<AbstractMatch<M, T>> toDelete = (List<AbstractMatch<M, T>>) testData;
        deleteAll(testDao, toDelete);
    }

    @Test
    public void persistCDMatchTestData() {
        CDMatch<EMNeuronMetadata, LMNeuronMetadata> testCDMatch =
                createTestCDMatch(
                        new AbstractNeuronMetadata.Builder<>(new EMNeuronMetadata())
                                .entityId(10)
                                .get(),
                        new AbstractNeuronMetadata.Builder<>(new LMNeuronMetadata())
                                .entityId(20)
                                .get(),
                        113
                );
        NeuronMatchesDao<EMNeuronMetadata, LMNeuronMetadata, CDMatch<EMNeuronMetadata, LMNeuronMetadata>> testDao =
                TestDBUtils.createNeuronMatchesDao(testMongoDatabase, idGenerator);
        testDao.save(testCDMatch);
        CDMatch<EMNeuronMetadata, LMNeuronMetadata> persistedCDMatch = testDao.findByEntityId(testCDMatch.getEntityId());
        assertNotNull(persistedCDMatch);
        assertNotSame(testCDMatch, persistedCDMatch);
        assertEquals(testCDMatch.getEntityId(), persistedCDMatch.getEntityId());
        assertNull(persistedCDMatch.getMaskImage());
        assertNull(persistedCDMatch.getMatchedImage());
        assertEquals(testCDMatch.getMaskImageRefId().toString(), persistedCDMatch.getMaskImageRefId().toString());
        assertEquals(testCDMatch.getMatchedImageRefId().toString(), persistedCDMatch.getMatchedImageRefId().toString());
    }

    @Test
    public void persistPPPMatchTestData() {
        PPPMatch<EMNeuronMetadata, LMNeuronMetadata> testPPPMatch =
                createTestPPPMatch(
                        new AbstractNeuronMetadata.Builder<>(new EMNeuronMetadata())
                                .entityId(10)
                                .get(),
                        new AbstractNeuronMetadata.Builder<>(new LMNeuronMetadata())
                                .entityId(20)
                                .get(),
                        0.5
                );
        NeuronMatchesDao<EMNeuronMetadata, LMNeuronMetadata, PPPMatch<EMNeuronMetadata, LMNeuronMetadata>> testDao =
                TestDBUtils.createNeuronMatchesDao(testMongoDatabase, idGenerator);
        testDao.save(testPPPMatch);
        PPPMatch<EMNeuronMetadata, LMNeuronMetadata> persistedPPPMatch = testDao.findByEntityId(testPPPMatch.getEntityId());
        assertNotNull(persistedPPPMatch);
        assertNotSame(testPPPMatch, persistedPPPMatch);
        assertEquals(testPPPMatch.getEntityId(), persistedPPPMatch.getEntityId());
        assertNull(persistedPPPMatch.getMaskImage());
        assertNull(persistedPPPMatch.getMatchedImage());
        assertEquals(testPPPMatch.getMaskImageRefId().toString(), persistedPPPMatch.getMaskImageRefId().toString());
        assertEquals(testPPPMatch.getMatchedImageRefId().toString(), persistedPPPMatch.getMatchedImageRefId().toString());
    }

    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata>
    CDMatch<M, T> createTestCDMatch(M maskNeuron, T targetNeuron, int pixelMatch) {
        CDMatch<M, T> testMatch = new CDMatch<>();
        testMatch.setMaskImage(maskNeuron);
        testMatch.setMatchedImage(targetNeuron);
        testMatch.setMatchingPixels(pixelMatch);
        addTestData(testMatch);
        return testMatch;
    }

    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata>
    PPPMatch<M, T> createTestPPPMatch(M maskNeuron, T targetNeuron, double rank) {
        PPPMatch<M, T> testMatch = new PPPMatch<>();
        testMatch.setMaskImage(maskNeuron);
        testMatch.setMatchedImage(targetNeuron);
        testMatch.setRank(rank);
        addTestData(testMatch);
        return testMatch;
    }

    @SuppressWarnings("unchecked")
    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata,R extends AbstractMatch<M, T>> void addTestData(R o) {
        List<AbstractMatch<M, T>> addTo = (List<AbstractMatch<M, T>>) testData;
        addTo.add(o);
    }
}
