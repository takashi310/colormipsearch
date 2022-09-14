package org.janelia.colormipsearch.dao.mongo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableSet;

import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.datarequests.PagedResult;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.EMNeuronEntity;
import org.janelia.colormipsearch.model.FileData;
import org.janelia.colormipsearch.model.LMNeuronEntity;
import org.janelia.colormipsearch.model.ProcessingType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public class NeuronMetadataMongoDaoITest extends AbstractMongoDaoITest {

    private final List<AbstractNeuronEntity> testData = new ArrayList<>();

    private NeuronMetadataDao<AbstractNeuronEntity> testDao;

    @Before
    public void setUp() {
        testDao = daosProvider.getNeuronMetadataDao();
    }

    @After
    public void tearDown() {
        deleteAll(testDao, testData);
    }

    @Test
    public void createOrUpdateEmNeuron() {
        EMNeuronEntity testEmNeuron = createTestNeuron(
                EMNeuronEntity::new,
                "flyem",
                "123445",
                "mip123",
                Collections.singleton("createOrUpdateEmNeuron"));
        AbstractNeuronEntity createdEmNeuron = testDao.createOrUpdate(testEmNeuron);
        assertEquals(testEmNeuron, createdEmNeuron);

        testEmNeuron.setComputeFileData(ComputeFileType.GradientImage, FileData.fromString("GradientImage"));
        testDao.createOrUpdate(testEmNeuron);

        AbstractNeuronEntity persistedEmNeuron = testDao.findByEntityId(testEmNeuron.getEntityId());
        assertEquals(testEmNeuron, persistedEmNeuron);
        assertNotSame(testEmNeuron, persistedEmNeuron);
    }

    @Test
    public void createOrUpdateNonIdentifiableEmNeuron() {
        EMNeuronEntity testEmNeuron = createTestNeuron(
                EMNeuronEntity::new,
                "flyem",
                "123445",
                "mip123",
                Collections.singleton("createOrUpdateNonIdentifiableEmNeuron"));
        EMNeuronEntity n1 = testEmNeuron.duplicate();
        n1.setComputeFileData(ComputeFileType.InputColorDepthImage, null); // reset input file
        testData.add(n1);
        EMNeuronEntity n2 = n1.duplicate();
        n1.setComputeFileData(ComputeFileType.InputColorDepthImage, null); // reset input file
        testData.add(n2);

        AbstractNeuronEntity createdEmNeuron = testDao.createOrUpdate(n1);
        assertEquals(n1, createdEmNeuron);

        testEmNeuron.setComputeFileData(ComputeFileType.GradientImage, FileData.fromString("GradientImage"));
        AbstractNeuronEntity anotherCreatedEmNeuron = testDao.createOrUpdate(n2);
        assertNotNull(createdEmNeuron.getEntityId());
        assertNotNull(anotherCreatedEmNeuron.getEntityId());
        assertNotEquals(createdEmNeuron.getEntityId(), anotherCreatedEmNeuron.getEntityId());

        AbstractNeuronEntity persistedEmNeuron = testDao.findByEntityId(n2.getEntityId());
        assertEquals(n2, persistedEmNeuron);
        assertNotSame(n2, persistedEmNeuron);
    }

    @Test
    public void saveFollowedByCreateOrUpdateEmNeuron() {
        EMNeuronEntity testEmNeuron = createTestNeuron(
                EMNeuronEntity::new,
                "flyem",
                "123445",
                "mip123",
                Collections.singleton("saveFollowedByCreateOrUpdateEmNeuron"));
        testDao.save(testEmNeuron);
        Number testEmId = testEmNeuron.getEntityId();

        testEmNeuron.setEntityId(null); // reset ID.
        testEmNeuron.setComputeFileData(ComputeFileType.GradientImage, FileData.fromString("GradientImage"));
        AbstractNeuronEntity updatedEmNeuron = testDao.createOrUpdate(testEmNeuron);
        assertEquals(testEmId, updatedEmNeuron.getEntityId());

        AbstractNeuronEntity persistedEmNeuron = testDao.findByEntityId(testEmNeuron.getEntityId());
        assertEquals(testEmId, persistedEmNeuron.getEntityId());

        assertEquals(testEmNeuron, persistedEmNeuron);
        assertNotSame(testEmNeuron, persistedEmNeuron);
    }

    @Test
    public void persistEmNeuron() {
        EMNeuronEntity testEmNeuron = createTestNeuron(
                EMNeuronEntity::new,
                "flyem",
                "123445",
                "mip123",
                Collections.singleton("persistEmNeuron"));
        testDao.save(testEmNeuron);
        AbstractNeuronEntity persistedEmNeuron = testDao.findByEntityId(testEmNeuron.getEntityId());
        assertEquals(testEmNeuron, persistedEmNeuron);
        assertNotSame(testEmNeuron, persistedEmNeuron);
    }

    @Test
    public void persistLmNeuron() {
        LMNeuronEntity testLmNeuron = createTestNeuron(
                LMNeuronEntity::new,
                "flylight_mcfo",
                "123445",
                "mip123",
                Collections.singleton("persistLmNeuron"));
        testDao.save(testLmNeuron);
        AbstractNeuronEntity persistedLmNeuron = testDao.findByEntityId(testLmNeuron.getEntityId());
        assertEquals(testLmNeuron, persistedLmNeuron);
        assertNotSame(testLmNeuron, persistedLmNeuron);
    }

    @Test
    public void findByLibrary() {
        String testLibrary = "flyem";
        EMNeuronEntity testEmNeuron = createTestNeuron(
                EMNeuronEntity::new,
                testLibrary,
                "123445",
                "mip123",
                Collections.singleton("findByLibrary"));
        testDao.save(testEmNeuron);
        PagedResult<? extends AbstractNeuronEntity> persistedEmNeurons = testDao.findNeurons(
                new NeuronSelector().addLibrary(testLibrary),
                new PagedRequest());
        assertEquals(1, persistedEmNeurons.getResultList().size());
        AbstractNeuronEntity persistedEmNeuron = persistedEmNeurons.getResultList().get(0);
        assertEquals(testEmNeuron, persistedEmNeuron);
        assertNotSame(testEmNeuron, persistedEmNeuron);
    }

    @Test
    public void findByLibraryAndType() {
        String testLibrary = "flyem";
        EMNeuronEntity testEmNeuron = createTestNeuron(
                EMNeuronEntity::new,
                testLibrary,
                "123445",
                "mip123",
                Collections.singleton("findByLibraryAndType"));
        testDao.save(testEmNeuron);
        PagedResult<? extends AbstractNeuronEntity> persistedEmNeurons = testDao.findNeurons(
                new NeuronSelector().addLibrary(testLibrary).setNeuronClassname(EMNeuronEntity.class.getName()),
                new PagedRequest());
        assertEquals(1, persistedEmNeurons.getResultList().size());
        AbstractNeuronEntity persistedEmNeuron = persistedEmNeurons.getResultList().get(0);
        assertEquals(testEmNeuron, persistedEmNeuron);
        assertNotSame(testEmNeuron, persistedEmNeuron);
    }

    @Test
    public void findDistinctNeurons() {
        String testLibrary = "flyem";
        EMNeuronEntity testEmNeuron = createTestNeuron(
                EMNeuronEntity::new,
                testLibrary,
                "123445",
                "mip123",
                Collections.singleton("findDistinctNeurons"));
        int nNeurons = 3;
        for (int i = 0; i < nNeurons; i++) {
            EMNeuronEntity n = testEmNeuron.duplicate();
            testData.add(n);
            testDao.save(n);
        }
        PagedResult<Map<String, Object>> distinctNeurons = testDao.findDistinctNeuronAttributeValues(
                Collections.singletonList("mipId"),
                new NeuronSelector().addLibrary(testLibrary).setNeuronClassname(EMNeuronEntity.class.getName()),
                new PagedRequest());
        assertEquals(1, distinctNeurons.getResultList().size());
    }

    @Test
    public void addProcessingTags() {
        String testLibrary = "flyem";
        int nNeurons = 3;
        List<Number> nIds = new ArrayList<>();
        for (int i = 0; i < nNeurons; i++) {
            EMNeuronEntity n = createTestNeuron(
                    EMNeuronEntity::new,
                    testLibrary,
                    "1234456",
                    "mip1234",
                    Collections.singleton("addProcessingTags"));
            testDao.save(n);
            nIds.add(n.getEntityId());
        }
        int iterations = 3;
        for (int iter = 0; iter < iterations; iter++) {
            Set<String> colorDepthTags = ImmutableSet.of("cd1-" + (iter+1), "cd2-" + (iter+1));
            Set<String> pppTags = ImmutableSet.of("ppp1-" + (iter+1), "ppp2-" + (iter+1));
            testDao.addProcessingTags(nIds, ProcessingType.ColorDepthSearch, colorDepthTags);
            testDao.addProcessingTags(nIds, ProcessingType.PPPMatch, pppTags);
            List<AbstractNeuronEntity> persistedNeurons = testDao.findByEntityIds(nIds);
            assertEquals(nNeurons, persistedNeurons.size());
            persistedNeurons.forEach(n -> {
                assertTrue(n.hasProcessedTags(ProcessingType.ColorDepthSearch, colorDepthTags));
                assertTrue(n.hasProcessedTags(ProcessingType.PPPMatch, pppTags));
            });
        }
    }

    private <N extends AbstractNeuronEntity> N createTestNeuron(Supplier<N> neuronGenerator,
                                                                String libraryName,
                                                                String name,
                                                                String mipId,
                                                                Collection<String> tags) {
        N testNeuron = new TestNeuronEntityBuilder<>(neuronGenerator)
                .library(libraryName)
                .publishedName(name)
                .mipId(mipId)
                .addTags(tags)
                .computeFileData(ComputeFileType.InputColorDepthImage, FileData.fromString("mipSegmentation"))
                .computeFileData(ComputeFileType.SourceColorDepthImage, FileData.fromString("sourceMip"))
                .get();
        testData.add(testNeuron);
        return testNeuron;
    }
}
