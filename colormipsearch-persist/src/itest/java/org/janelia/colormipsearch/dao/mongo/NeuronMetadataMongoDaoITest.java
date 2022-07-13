package org.janelia.colormipsearch.dao.mongo;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.EMNeuronMetadata;
import org.janelia.colormipsearch.model.FileData;
import org.janelia.colormipsearch.model.FileType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class NeuronMetadataMongoDaoITest extends AbstractMongoDaoITest {

    private final List<AbstractNeuronMetadata> testData = new ArrayList<>();

    private NeuronMetadataDao<AbstractNeuronMetadata> testDao;

    @Before
    public void setUp() {
        testDao = TestDBUtils.createNeuronMetadataDao(testMongoDatabase, idGenerator);
    }

    @After
    public void tearDown() {
        deleteAll(testDao, testData);
    }

    @Test
    public void persistEmNeuron() {
        EMNeuronMetadata testEmNeuron = createTestNeuron(
                EMNeuronMetadata::new,
                "flyem",
                "123445");
        testDao.save(testEmNeuron);
        AbstractNeuronMetadata persistedEmNeuron = testDao.findByEntityId(testEmNeuron.getEntityId());
        assertEquals(testEmNeuron, persistedEmNeuron);
        assertNotSame(testEmNeuron, persistedEmNeuron);
    }

    private <N extends AbstractNeuronMetadata> N createTestNeuron(Supplier<N> neuronGenerator,
                                                                  String libraryName,
                                                                  String name) {
        N testNeuron = new AbstractNeuronMetadata.Builder<>(neuronGenerator)
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
