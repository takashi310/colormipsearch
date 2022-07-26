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
import org.janelia.colormipsearch.dao.NeuronsMatchFilter;
import org.janelia.colormipsearch.datarequests.PagedRequest;
import org.janelia.colormipsearch.model.AbstractBaseEntity;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.EMNeuronEntity;
import org.janelia.colormipsearch.model.FileData;
import org.janelia.colormipsearch.model.FileType;
import org.janelia.colormipsearch.model.LMNeuronEntity;
import org.janelia.colormipsearch.model.PPPMatch;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public class NeuronMatchesMongoDaoITest extends AbstractMongoDaoITest {

    private final List<? extends AbstractMatch<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> testData = new ArrayList<>();

    @After
    public <M extends AbstractNeuronEntity,
            T extends AbstractNeuronEntity>
    void tearDown() {
        // delete the data that was created for testing
        NeuronMatchesDao<AbstractMatch<M, T>> neuronMatchesDao = daosProvider.getNeuronMatchesDao();
        @SuppressWarnings("unchecked")
        List<AbstractMatch<M, T>> toDelete = (List<AbstractMatch<M, T>>) testData;
        deleteAll(neuronMatchesDao, toDelete);
    }

    @Test
    public void persistCDMatchWithDummyImages() {
        CDMatch<EMNeuronEntity, LMNeuronEntity> testCDMatch =
                createTestCDMatch(
                        new AbstractNeuronEntity.Builder<>(EMNeuronEntity::new)
                                .entityId(10)
                                .get(),
                        new AbstractNeuronEntity.Builder<>(LMNeuronEntity::new)
                                .entityId(20)
                                .get(),
                        113,
                        .5
                );
        NeuronMatchesDao<CDMatch<EMNeuronEntity, LMNeuronEntity>> neuronMatchesDao =
                daosProvider.getNeuronMatchesDao();
        neuronMatchesDao.save(testCDMatch);
        CDMatch<EMNeuronEntity, LMNeuronEntity> persistedCDMatch = neuronMatchesDao.findByEntityId(testCDMatch.getEntityId());
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
        NeuronMatchesDao<CDMatch<EMNeuronEntity, LMNeuronEntity>> neuronMatchesDao =
                daosProvider.getNeuronMatchesDao();

        verifyMultipleCDMatcheshWithImages(
                1,
                neuronMatchesDao,
                ms -> Collections.singletonList(neuronMatchesDao.findByEntityId(ms.get(0).getEntityId())));
    }

    @Test
    public void findMultipleCDMatchesByEntityIds() {
        NeuronMatchesDao<CDMatch<EMNeuronEntity, LMNeuronEntity>> neuronMatchesDao =
                daosProvider.getNeuronMatchesDao();

        verifyMultipleCDMatcheshWithImages(
                20,
                neuronMatchesDao,
                ms -> neuronMatchesDao.findByEntityIds(
                        ms.stream()
                                .map(AbstractBaseEntity::getEntityId)
                                .collect(Collectors.toList())
                )
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void findAllCDMatchesWithoutPagination() {
        NeuronMatchesDao<CDMatch<EMNeuronEntity, LMNeuronEntity>> neuronMatchesDao =
                daosProvider.getNeuronMatchesDao();

        Class<?> searchedType = CDMatch.class;
        verifyMultipleCDMatcheshWithImages(
                20,
                neuronMatchesDao,
                ms -> neuronMatchesDao.findAll((Class<CDMatch<EMNeuronEntity, LMNeuronEntity>>) searchedType, new PagedRequest()).getResultList()
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void findAllCDMatchesWithPagination() {
        NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao = daosProvider.getNeuronMetadataDao();
        Pair<EMNeuronEntity, LMNeuronEntity> neuronImages = createMatchingImages(neuronMetadataDao);
        NeuronMatchesDao<CDMatch<EMNeuronEntity, LMNeuronEntity>> neuronMatchesDao =
                daosProvider.getNeuronMatchesDao();
        int nTestMatches = 27;
        List<CDMatch<EMNeuronEntity, LMNeuronEntity>> testCDMatches = createTestCDMatches(nTestMatches, neuronImages, neuronMatchesDao);
        try {
            int pageSize = 5;
            Class<?> searchedType = CDMatch.class;
            for (int i = 0; i < nTestMatches; i += pageSize) {
                int page = i / pageSize;
                retrieveAndCompareCDMatcheshWithImages(
                        testCDMatches.subList(i, Math.min(testCDMatches.size(), i + pageSize)),
                        ms -> neuronMatchesDao.findAll(
                                (Class<CDMatch<EMNeuronEntity, LMNeuronEntity>>) searchedType,
                                new PagedRequest().setPageNumber(page).setPageSize(pageSize)).getResultList());
            }
        } finally {
            deleteAll(neuronMetadataDao, Arrays.asList(neuronImages.getLeft(), neuronImages.getRight()));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void findCDMatchesUsingNeuronSelectors() {
        NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao = daosProvider.getNeuronMetadataDao();
        Pair<EMNeuronEntity, LMNeuronEntity> neuronImages = createMatchingImages(neuronMetadataDao);
        NeuronMatchesDao<CDMatch<EMNeuronEntity, LMNeuronEntity>> neuronMatchesDao =
                daosProvider.getNeuronMatchesDao();
        int nTestMatches = 27;
        List<CDMatch<EMNeuronEntity, LMNeuronEntity>> testCDMatches = createTestCDMatches(nTestMatches, neuronImages, neuronMatchesDao);
        try {
            int pageSize = 5;
            NeuronsMatchFilter<CDMatch<EMNeuronEntity, LMNeuronEntity>> matchesFilter =
                    new NeuronsMatchFilter<CDMatch<EMNeuronEntity, LMNeuronEntity>>();
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
                        ms -> neuronMatchesDao.findNeuronMatches(
                                matchesFilter,
                                emNeuronSelector,
                                lmNeuronSelector,
                                pagedRequest).getResultList());
            }
            // retrieve all
            retrieveAndCompareCDMatcheshWithImages(
                    testCDMatches,
                    ms -> neuronMatchesDao.findNeuronMatches(
                            matchesFilter,
                            emNeuronSelector,
                            lmNeuronSelector,
                            new PagedRequest()).getResultList());
            // retrieve none
            retrieveAndCompareCDMatcheshWithImages(
                    Collections.emptyList(),
                    ms -> neuronMatchesDao.findNeuronMatches(
                            matchesFilter,
                            lmNeuronSelector,
                            emNeuronSelector,
                            new PagedRequest()).getResultList());
            assertEquals(
                    nTestMatches,
                    neuronMatchesDao.countNeuronMatches(
                            matchesFilter,
                            emNeuronSelector,
                            lmNeuronSelector));
            assertEquals(
                    0,
                    neuronMatchesDao.countNeuronMatches(
                            matchesFilter,
                            lmNeuronSelector,
                            emNeuronSelector));
        } finally {
            deleteAll(neuronMetadataDao, Arrays.asList(neuronImages.getLeft(), neuronImages.getRight()));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void findAndUpdate() {
        NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao = daosProvider.getNeuronMetadataDao();
        Pair<EMNeuronEntity, LMNeuronEntity> neuronImages = createMatchingImages(neuronMetadataDao);
        try {

            NeuronMatchesDao<CDMatch<EMNeuronEntity, LMNeuronEntity>> neuronMatchesDao =
                    daosProvider.getNeuronMatchesDao();

            CDMatch<EMNeuronEntity, LMNeuronEntity> testCDMatch =
                    createTestCDMatch(neuronImages.getLeft(), neuronImages.getRight(), 113, 0.76);

            List<CDMatch<EMNeuronEntity, LMNeuronEntity>> testCDMatches = Collections.singletonList(testCDMatch);

            List<Function<CDMatch<EMNeuronEntity, LMNeuronEntity>, Pair<String, ?>>> fieldsToUpdate = Arrays.asList(
                    m -> ImmutablePair.of("matchingPixels", m.getMatchingPixels()),
                    m -> ImmutablePair.of("matchingPixelsRatio", m.getMatchingPixelsRatio()),
                    m -> ImmutablePair.of("gradientAreaGap", m.getGradientAreaGap()),
                    m -> ImmutablePair.of("highExpressionArea", m.getHighExpressionArea()),
                    m -> ImmutablePair.of("normalizedScore", m.getNormalizedScore())
            );

            // save the matches multiple times and check the count is still 1
            for (int i = 0; i < 3; i++) {
                testCDMatch.setMatchingPixels(testCDMatch.getMatchingPixels() + i);
                testCDMatch.setMatchingPixelsRatio(testCDMatch.getMatchingPixelsRatio() / (i + 1));
                neuronMatchesDao.createOrUpdateAll(testCDMatches, fieldsToUpdate);
                // check that it was saved
                assertEquals(
                        1,
                        neuronMatchesDao.countNeuronMatches(null, null, null)
                );
            }
        } finally {
            deleteAll(neuronMetadataDao, Arrays.asList(neuronImages.getLeft(), neuronImages.getRight()));
        }
    }

    private void verifyMultipleCDMatcheshWithImages(int nTestMatches,
                                                    NeuronMatchesDao<CDMatch<EMNeuronEntity, LMNeuronEntity>> neuronMatchesDao,
                                                    Function<List<CDMatch<EMNeuronEntity, LMNeuronEntity>>,
                                                            List<CDMatch<EMNeuronEntity, LMNeuronEntity>>> dataRetriever) {
        NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao =
                daosProvider.getNeuronMetadataDao();
        Pair<EMNeuronEntity, LMNeuronEntity> neuronImages = createMatchingImages(neuronMetadataDao);
        try {
            List<CDMatch<EMNeuronEntity, LMNeuronEntity>> testCDMatches = createTestCDMatches(nTestMatches, neuronImages, neuronMatchesDao);
            retrieveAndCompareCDMatcheshWithImages(testCDMatches, dataRetriever);
        } finally {
            deleteAll(neuronMetadataDao, Arrays.asList(neuronImages.getLeft(), neuronImages.getRight()));
        }
    }

    private Pair<EMNeuronEntity, LMNeuronEntity> createMatchingImages(NeuronMetadataDao<AbstractNeuronEntity> neuronMetadataDao) {
        EMNeuronEntity emNeuronMetadata = new AbstractNeuronEntity.Builder<>(EMNeuronEntity::new)
                .id("123232232423232")
                .publishedName("23232345")
                .library("FlyEM Hemibrain")
                .computeFileData(ComputeFileType.InputColorDepthImage, FileData.fromString("mask-mipSegmentation"))
                .computeFileData(ComputeFileType.SourceColorDepthImage, FileData.fromString("mask-sourceMip"))
                .fileData(FileType.ColorDepthMip, FileData.fromString("mask-cdmip"))
                .fileData(FileType.ColorDepthMipInput, FileData.fromString("mask-cdmipInput"))
                .get();
        LMNeuronEntity lmNeuronMetadata = new AbstractNeuronEntity.Builder<>(LMNeuronEntity::new)
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

    private List<CDMatch<EMNeuronEntity, LMNeuronEntity>> createTestCDMatches(
            int nTestMatches,
            Pair<EMNeuronEntity, LMNeuronEntity> matchingImages,
            NeuronMatchesDao<CDMatch<EMNeuronEntity, LMNeuronEntity>> neuronMatchesDao) {
        List<CDMatch<EMNeuronEntity, LMNeuronEntity>> testCDMatches = new ArrayList<>();
        for (int i = 0; i < nTestMatches; i++) {
            CDMatch<EMNeuronEntity, LMNeuronEntity> testCDMatch =
                    createTestCDMatch(matchingImages.getLeft(), matchingImages.getRight(), 113 + i, 0.76 / i);
            neuronMatchesDao.save(testCDMatch);
            testCDMatches.add(testCDMatch);
        }
        return testCDMatches;
    }

    private void retrieveAndCompareCDMatcheshWithImages(
            List<CDMatch<EMNeuronEntity, LMNeuronEntity>> testCDMatches,
            Function<List<CDMatch<EMNeuronEntity, LMNeuronEntity>>,
                    List<CDMatch<EMNeuronEntity, LMNeuronEntity>>> dataRetriever) {
        List<CDMatch<EMNeuronEntity, LMNeuronEntity>> persistedCDMatches = dataRetriever.apply(testCDMatches);
        assertEquals(testCDMatches.size(), persistedCDMatches.size());
        persistedCDMatches.sort(Comparator.comparingLong(m -> m.getEntityId().longValue()));
        for (int i = 0; i < testCDMatches.size(); i++) {
            CDMatch<EMNeuronEntity, LMNeuronEntity> testCDMatch = testCDMatches.get(i);
            CDMatch<EMNeuronEntity, LMNeuronEntity> persistedCDMatch = persistedCDMatches.get(i);
            assertEquals(testCDMatch.getEntityId(), persistedCDMatch.getEntityId());
            assertEquals(testCDMatch.getMaskImage(), persistedCDMatch.getMaskImage());
            assertEquals(testCDMatch.getMatchedImage(), persistedCDMatch.getMatchedImage());
            assertEquals(testCDMatch.getCreatedDate(), persistedCDMatch.getCreatedDate());
        }
    }

    @Test
    public void persistPPPMatchTestData() {
        PPPMatch<EMNeuronEntity, LMNeuronEntity> testPPPMatch =
                createTestPPPMatch(
                        new AbstractNeuronEntity.Builder<>(EMNeuronEntity::new)
                                .entityId(10)
                                .get(),
                        new AbstractNeuronEntity.Builder<>(LMNeuronEntity::new)
                                .entityId(20)
                                .get(),
                        0.5
                );
        NeuronMatchesDao<PPPMatch<EMNeuronEntity, LMNeuronEntity>> neuronMatchesDao =
                daosProvider.getNeuronMatchesDao();
        neuronMatchesDao.save(testPPPMatch);
        PPPMatch<EMNeuronEntity, LMNeuronEntity> persistedPPPMatch = neuronMatchesDao.findByEntityId(testPPPMatch.getEntityId());
        assertNotNull(persistedPPPMatch);
        assertNotSame(testPPPMatch, persistedPPPMatch);
        assertEquals(testPPPMatch.getEntityId(), persistedPPPMatch.getEntityId());
        assertNull(persistedPPPMatch.getMaskImage());
        assertNull(persistedPPPMatch.getMatchedImage());
        assertEquals(testPPPMatch.getMaskImageRefId().toString(), persistedPPPMatch.getMaskImageRefId().toString());
        assertEquals(testPPPMatch.getMatchedImageRefId().toString(), persistedPPPMatch.getMatchedImageRefId().toString());
    }

    private <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity>
    CDMatch<M, T> createTestCDMatch(M maskNeuron, T targetNeuron, int pixelMatch, double pixelMatchRatio) {
        CDMatch<M, T> testMatch = new CDMatch<>();
        testMatch.setMaskImage(maskNeuron);
        testMatch.setMatchedImage(targetNeuron);
        testMatch.setMatchingPixels(pixelMatch);
        testMatch.setMatchingPixelsRatio((float) pixelMatchRatio);
        addTestData(testMatch);
        return testMatch;
    }

    private <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity>
    PPPMatch<M, T> createTestPPPMatch(M maskNeuron, T targetNeuron, double rank) {
        PPPMatch<M, T> testMatch = new PPPMatch<>();
        testMatch.setMaskImage(maskNeuron);
        testMatch.setMatchedImage(targetNeuron);
        testMatch.setRank(rank);
        addTestData(testMatch);
        return testMatch;
    }

    @SuppressWarnings("unchecked")
    private <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity, R extends AbstractMatch<M, T>> void addTestData(R o) {
        ((List<AbstractMatch<M, T>>) testData).add(o);
    }
}
