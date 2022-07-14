package org.janelia.colormipsearch.dao.mongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.dao.NeuronMatchesDao;
import org.janelia.colormipsearch.dao.NeuronMetadataDao;
import org.janelia.colormipsearch.dao.NeuronSelector;
import org.janelia.colormipsearch.dao.PagedRequest;
import org.janelia.colormipsearch.model.AbstractBaseEntity;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.EMNeuronMetadata;
import org.janelia.colormipsearch.model.FileData;
import org.janelia.colormipsearch.model.FileType;
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
    public void persistCDMatchWithDummyImages() {
        CDMatch<EMNeuronMetadata, LMNeuronMetadata> testCDMatch =
                createTestCDMatch(
                        new AbstractNeuronMetadata.Builder<>(EMNeuronMetadata::new)
                                .entityId(10)
                                .get(),
                        new AbstractNeuronMetadata.Builder<>(LMNeuronMetadata::new)
                                .entityId(20)
                                .get(),
                        113,
                        .5
                );
        NeuronMatchesDao<EMNeuronMetadata, LMNeuronMetadata, CDMatch<EMNeuronMetadata, LMNeuronMetadata>> testDao =
                TestDBUtils.createNeuronMatchesDao(testMongoDatabase, idGenerator);
        testDao.save(testCDMatch);
        CDMatch<EMNeuronMetadata, LMNeuronMetadata> persistedCDMatch = testDao.findByEntityId(testCDMatch.getEntityId());
        assertNotNull(persistedCDMatch);
        assertNotSame(testCDMatch, persistedCDMatch);
        assertEquals(testCDMatch.getEntityId(), persistedCDMatch.getEntityId());
        assertNull(persistedCDMatch.getMatchedImage());
        assertNull(persistedCDMatch.getMaskImage());
        assertEquals(testCDMatch.getMaskImageRefId().toString(), persistedCDMatch.getMaskImageRefId().toString());
        assertEquals(testCDMatch.getMatchedImageRefId().toString(), persistedCDMatch.getMatchedImageRefId().toString());
    }

    @Test
    public void persistCDMatchWithImages() {
        NeuronMatchesDao<EMNeuronMetadata, LMNeuronMetadata, CDMatch<EMNeuronMetadata, LMNeuronMetadata>> testDao =
                TestDBUtils.createNeuronMatchesDao(testMongoDatabase, idGenerator);

        verifyMultipleCDMatcheshWithImages(
                1,
                testDao,
                ms -> Collections.singletonList(testDao.findByEntityId(ms.get(0).getEntityId())));
    }

    @Test
    public void findMultipleCDMatchesByEntityIds() {
        NeuronMatchesDao<EMNeuronMetadata, LMNeuronMetadata, CDMatch<EMNeuronMetadata, LMNeuronMetadata>> testDao =
                TestDBUtils.createNeuronMatchesDao(testMongoDatabase, idGenerator);

        verifyMultipleCDMatcheshWithImages(
                20,
                testDao,
                ms -> testDao.findByEntityIds(
                        ms.stream()
                                .map(AbstractBaseEntity::getEntityId)
                                .collect(Collectors.toList())
                )
        );
    }

    @Test
    public void findAllCDMatchesWithoutPagination() {
        NeuronMatchesDao<EMNeuronMetadata, LMNeuronMetadata, CDMatch<EMNeuronMetadata, LMNeuronMetadata>> testDao =
                TestDBUtils.createNeuronMatchesDao(testMongoDatabase, idGenerator);

        verifyMultipleCDMatcheshWithImages(
                20,
                testDao,
                ms -> testDao.findAll(new PagedRequest()).getResultList()
        );
    }

    @Test
    public void findAllCDMatchesWithPagination() {
        NeuronMetadataDao<AbstractNeuronMetadata> neuronMetadataDao =
                TestDBUtils.createNeuronMetadataDao(testMongoDatabase, idGenerator);
        Pair<EMNeuronMetadata, LMNeuronMetadata> neuronImages = createMatchingImages(neuronMetadataDao);
        NeuronMatchesDao<EMNeuronMetadata, LMNeuronMetadata, CDMatch<EMNeuronMetadata, LMNeuronMetadata>> testDao =
                TestDBUtils.createNeuronMatchesDao(testMongoDatabase, idGenerator);
        int nTestMatches = 27;
        List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> testCDMatches = createTestCDMatches(nTestMatches, neuronImages, testDao);
        try {
            int pageSize = 5;
            for (int i = 0; i < nTestMatches; i += pageSize) {
                int page = i / pageSize;
                retrieveAndCompareCDMatcheshWithImages(
                        testCDMatches.subList(i, Math.min(testCDMatches.size(), i + pageSize)),
                        ms -> testDao.findAll(new PagedRequest().setPageNumber(page).setPageSize(pageSize)).getResultList());

            }
        } finally {
            deleteAll(neuronMetadataDao, Arrays.asList(neuronImages.getLeft(), neuronImages.getRight()));
        }
    }

    @Test
    public void findCDMatchesUsingNeuronSelectors() {
        NeuronMetadataDao<AbstractNeuronMetadata> neuronMetadataDao =
                TestDBUtils.createNeuronMetadataDao(testMongoDatabase, idGenerator);
        Pair<EMNeuronMetadata, LMNeuronMetadata> neuronImages = createMatchingImages(neuronMetadataDao);
        NeuronMatchesDao<EMNeuronMetadata, LMNeuronMetadata, CDMatch<EMNeuronMetadata, LMNeuronMetadata>> testDao =
                TestDBUtils.createNeuronMatchesDao(testMongoDatabase, idGenerator);
        int nTestMatches = 27;
        List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> testCDMatches = createTestCDMatches(nTestMatches, neuronImages, testDao);
        try {
            int pageSize = 5;
            NeuronSelector emNeuronSelector = new NeuronSelector()
                    .setLibraryName("FlyEM Hemibrain")
                    .addMipID("123232232423232")
                    .addName("23232345");
            NeuronSelector lmNeuronSelector = new NeuronSelector()
                    .setLibraryName("Split GAL4")
                    .addMipID("5565655454545432")
                    .addName("S1234");
            for (int i = 0; i < nTestMatches; i += pageSize) {
                int page = i / pageSize;
                PagedRequest pagedRequest = new PagedRequest().setPageNumber(page).setPageSize(pageSize);
                retrieveAndCompareCDMatcheshWithImages(
                        testCDMatches.subList(i, Math.min(testCDMatches.size(), i + pageSize)),
                        ms -> testDao.findNeuronMatches(
                                emNeuronSelector,
                                lmNeuronSelector,
                                pagedRequest).getResultList());
            }
            // retrieve all
            retrieveAndCompareCDMatcheshWithImages(
                    testCDMatches,
                    ms -> testDao.findNeuronMatches(
                            emNeuronSelector,
                            lmNeuronSelector,
                            new PagedRequest()).getResultList());
            // retrieve none
            retrieveAndCompareCDMatcheshWithImages(
                    Collections.emptyList(),
                    ms -> testDao.findNeuronMatches(
                            lmNeuronSelector,
                            emNeuronSelector,
                            new PagedRequest()).getResultList());
            assertEquals(nTestMatches, testDao.countNeuronMatches(emNeuronSelector, lmNeuronSelector));
            assertEquals(0, testDao.countNeuronMatches(lmNeuronSelector, emNeuronSelector));
        } finally {
            deleteAll(neuronMetadataDao, Arrays.asList(neuronImages.getLeft(), neuronImages.getRight()));
        }
    }

    private void verifyMultipleCDMatcheshWithImages(int nTestMatches,
                                                    NeuronMatchesDao<EMNeuronMetadata, LMNeuronMetadata, CDMatch<EMNeuronMetadata, LMNeuronMetadata>> neuronMatchesDao,
                                                    Function<List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>>,
                                                            List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>>> dataRetriever) {
        NeuronMetadataDao<AbstractNeuronMetadata> neuronMetadataDao =
                TestDBUtils.createNeuronMetadataDao(testMongoDatabase, idGenerator);
        Pair<EMNeuronMetadata, LMNeuronMetadata> neuronImages = createMatchingImages(neuronMetadataDao);
        try {
            List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> testCDMatches = createTestCDMatches(nTestMatches, neuronImages, neuronMatchesDao);
            retrieveAndCompareCDMatcheshWithImages(testCDMatches, dataRetriever);
        } finally {
            deleteAll(neuronMetadataDao, Arrays.asList(neuronImages.getLeft(), neuronImages.getRight()));
        }
    }

    private Pair<EMNeuronMetadata, LMNeuronMetadata> createMatchingImages(NeuronMetadataDao<AbstractNeuronMetadata> neuronMetadataDao) {
        EMNeuronMetadata emNeuronMetadata = new AbstractNeuronMetadata.Builder<>(EMNeuronMetadata::new)
                .id("123232232423232")
                .publishedName("23232345")
                .library("FlyEM Hemibrain")
                .computeFileData(ComputeFileType.InputColorDepthImage, FileData.fromString("mask-mipSegmentation"))
                .computeFileData(ComputeFileType.SourceColorDepthImage, FileData.fromString("mask-sourceMip"))
                .fileData(FileType.ColorDepthMip, FileData.fromString("mask-cdmip"))
                .fileData(FileType.ColorDepthMipInput, FileData.fromString("mask-cdmipInput"))
                .get();
        LMNeuronMetadata lmNeuronMetadata = new AbstractNeuronMetadata.Builder<>(LMNeuronMetadata::new)
                .id("5565655454545432")
                .publishedName("S1234")
                .library("Split GAL4")
                .computeFileData(ComputeFileType.InputColorDepthImage, FileData.fromString("match-mipSegmentation"))
                .computeFileData(ComputeFileType.SourceColorDepthImage, FileData.fromString("match-sourceMip"))
                .fileData(FileType.ColorDepthMip, FileData.fromString("match-cdmip"))
                .fileData(FileType.ColorDepthMipInput, FileData.fromString("match-cdmipInput"))
                .get();

        neuronMetadataDao.save(lmNeuronMetadata);
        neuronMetadataDao.save(emNeuronMetadata);
        return ImmutablePair.of(emNeuronMetadata, lmNeuronMetadata);
    }

    private List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> createTestCDMatches(
            int nTestMatches,
            Pair<EMNeuronMetadata, LMNeuronMetadata> matchingImages,
            NeuronMatchesDao<EMNeuronMetadata, LMNeuronMetadata, CDMatch<EMNeuronMetadata, LMNeuronMetadata>> neuronMatchesDao) {
        List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> testCDMatches = new ArrayList<>();
        for (int i = 0; i < nTestMatches; i++) {
            CDMatch<EMNeuronMetadata, LMNeuronMetadata> testCDMatch =
                    createTestCDMatch(matchingImages.getLeft(), matchingImages.getRight(), 113 + i, 0.76 / i);
            neuronMatchesDao.save(testCDMatch);
            testCDMatches.add(testCDMatch);
        }
        return testCDMatches;
    }

    private void retrieveAndCompareCDMatcheshWithImages(
            List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> testCDMatches,
            Function<List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>>,
                    List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>>> dataRetriever) {
        List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> persistedCDMatches = dataRetriever.apply(testCDMatches);
        assertEquals(testCDMatches.size(), persistedCDMatches.size());
        persistedCDMatches.sort(Comparator.comparingLong(m -> m.getEntityId().longValue()));
        for (int i = 0; i < testCDMatches.size(); i++) {
            CDMatch<EMNeuronMetadata, LMNeuronMetadata> testCDMatch = testCDMatches.get(i);
            CDMatch<EMNeuronMetadata, LMNeuronMetadata> persistedCDMatch = persistedCDMatches.get(i);
            assertEquals(testCDMatch.getEntityId(), persistedCDMatch.getEntityId());
            assertEquals(testCDMatch.getMaskImage(), persistedCDMatch.getMaskImage());
            assertEquals(testCDMatch.getMatchedImage(), persistedCDMatch.getMatchedImage());
            assertEquals(testCDMatch.getCreatedDate(), persistedCDMatch.getCreatedDate());
        }
    }

    @Test
    public void persistPPPMatchTestData() {
        PPPMatch<EMNeuronMetadata, LMNeuronMetadata> testPPPMatch =
                createTestPPPMatch(
                        new AbstractNeuronMetadata.Builder<>(EMNeuronMetadata::new)
                                .entityId(10)
                                .get(),
                        new AbstractNeuronMetadata.Builder<>(LMNeuronMetadata::new)
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
    CDMatch<M, T> createTestCDMatch(M maskNeuron, T targetNeuron, int pixelMatch, double pixelMatchRatio) {
        CDMatch<M, T> testMatch = new CDMatch<>();
        testMatch.setMaskImage(maskNeuron);
        testMatch.setMatchedImage(targetNeuron);
        testMatch.setMatchingPixels(pixelMatch);
        testMatch.setMatchingPixelsRatio((float) pixelMatchRatio);
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
    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>> void addTestData(R o) {
        List<AbstractMatch<M, T>> addTo = (List<AbstractMatch<M, T>>) testData;
        addTo.add(o);
    }
}
