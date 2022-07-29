package org.janelia.colormipsearch.dao.mongo;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.datarequests.PagedResult;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.EMNeuronEntity;
import org.janelia.colormipsearch.model.FileData;
import org.janelia.colormipsearch.model.FileType;
import org.janelia.colormipsearch.model.LMNeuronEntity;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

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
                "mip123");
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
                "mip123");
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
                "mip123");
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
                "mip123");
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
                "mip123");
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
                "mip123");
        testDao.save(testEmNeuron);
        PagedResult<? extends AbstractNeuronEntity> persistedEmNeurons = testDao.findNeurons(
                new NeuronSelector().setLibraryName(testLibrary),
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
                "mip123");
        testDao.save(testEmNeuron);
        PagedResult<? extends AbstractNeuronEntity> persistedEmNeurons = testDao.findNeurons(
                new NeuronSelector().setLibraryName(testLibrary).setNeuronClassname(EMNeuronEntity.class.getName()),
                new PagedRequest());
        assertEquals(1, persistedEmNeurons.getResultList().size());
        AbstractNeuronEntity persistedEmNeuron = persistedEmNeurons.getResultList().get(0);
        assertEquals(testEmNeuron, persistedEmNeuron);
        assertNotSame(testEmNeuron, persistedEmNeuron);
    }

    private <N extends AbstractNeuronEntity> N createTestNeuron(Supplier<N> neuronGenerator,
                                                                String libraryName,
                                                                String name,
                                                                String mipId) {
        N testNeuron = new TestNeuronEntityBuilder<>(neuronGenerator)
                .library(libraryName)
                .publishedName(name)
                .mipId(mipId)
                .computeFileData(ComputeFileType.InputColorDepthImage, FileData.fromString("mipSegmentation"))
                .computeFileData(ComputeFileType.SourceColorDepthImage, FileData.fromString("sourceMip"))
                .fileData(FileType.ColorDepthMip, "cdmip")
                .fileData(FileType.ColorDepthMipInput, "cdmipInput")
                .get();
        testData.add(testNeuron);
        return testNeuron;
    }
}
