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
    public void persistEmNeuron() {
        EMNeuronEntity testEmNeuron = createTestNeuron(
                EMNeuronEntity::new,
                "flyem",
                "123445");
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
                "123445");
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
                "123445");
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
                "123445");
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
                                                                String name) {
        N testNeuron = new AbstractNeuronEntity.Builder<>(neuronGenerator)
                .library(libraryName)
                .publishedName(name)
                .computeFileData(ComputeFileType.InputColorDepthImage, FileData.fromString("mipSegmentation"))
                .computeFileData(ComputeFileType.SourceColorDepthImage, FileData.fromString("sourceMip"))
                .fileData(FileType.ColorDepthMip, FileData.fromString("cdmip"))
                .fileData(FileType.ColorDepthMipInput, FileData.fromString("cdmipInput"))
                .get();
        testData.add(testNeuron);
        return testNeuron;
    }
}
