package org.janelia.colormipsearch.results;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.model.EMNeuronEntity;
import org.janelia.colormipsearch.model.LMNeuronEntity;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ItemsHandlingTest {
    @Test
    public void partitionStream() {
        int[][] testData = new int[][] {
                { 100, 100},
                { 100, 25},
                { 101, 26},
                { 200, 2 },
                { 100, 1 },
                { 150, 200},
                { 200, 36 },
        };
        for (int[] td : testData) {
            int maxValue = td[0];
            int partitionSize = td[1];
            List<List<Integer>> listOfList = Collections.synchronizedList(new ArrayList<>());
            ItemsHandling.processPartitionStream(
                    IntStream.range(0, maxValue).boxed().parallel(),
                    partitionSize,
                    listOfList::add,
                    true
            );

            int exactPartitionAdjustment = maxValue % partitionSize == 0 ? 0 : 1;
            int nPartitions = maxValue / partitionSize + exactPartitionAdjustment;
            assertEquals("Test: " + Arrays.toString(td), nPartitions, listOfList.size());
            for (int i = 0; i < nPartitions-1; i++) {
                assertEquals("Test: " + Arrays.toString(td) + ": partition: " + (i+1), partitionSize, listOfList.get(i).size());
            }
            assertEquals(
                    "Test: " + Arrays.toString(td),
                    exactPartitionAdjustment == 0 ? partitionSize : maxValue % partitionSize,
                    listOfList.get(nPartitions-1).size()
            );
            List<Integer> concatenatedList = listOfList.stream().flatMap(l -> l.stream()).collect(Collectors.toList());
            assertNotEquals(
                    "Test: " + Arrays.toString(td),
                    IntStream.range(0, maxValue).boxed().collect(Collectors.toList()),
                    concatenatedList);
            concatenatedList.sort(Comparator.naturalOrder());
            assertEquals(
                    "Test: " + Arrays.toString(td),
                    IntStream.range(0, maxValue).boxed().collect(Collectors.toList()),
                    concatenatedList);
        }
    }

    @Test
    public void selectAllElementsWithAllSubResults() {
        List<CDMatch<EMNeuronEntity, LMNeuronEntity>> testData = createTestData();

        Map<String, List<CDMatch<EMNeuronEntity, LMNeuronEntity>>> testDataByLine = testData.stream().collect(Collectors.groupingBy(
                m -> m.getMatchedImage().getPublishedName(),
                Collectors.toList()
        ));

        List<ScoredEntry<List<CDMatch<EMNeuronEntity, LMNeuronEntity>>>> rankedLines = ItemsHandling.selectTopRankedElements(
                testData,
                match -> match.getMatchedImage().getPublishedName(),
                CDMatch::getMatchingPixels,
                -1,
                -1);

        assertEquals(testDataByLine.size(), rankedLines.size());

        rankedLines.forEach(lineMatches -> {
            assertEquals(testDataByLine.get(lineMatches.getName()).size(), lineMatches.getEntry().size());
        });
    }

    @Test
    public void selectAllElementsWithLimitedSubResults() {
        List<CDMatch<EMNeuronEntity, LMNeuronEntity>> testData = createTestData();

        Map<String, List<CDMatch<EMNeuronEntity, LMNeuronEntity>>> testDataByLine = testData.stream().collect(Collectors.groupingBy(
                m -> m.getMatchedImage().getPublishedName(),
                Collectors.toList()
        ));

        for (int si = 1; si <= 3; si++) {
            int topSubResults = si;
            List<ScoredEntry<List<CDMatch<EMNeuronEntity, LMNeuronEntity>>>> rankedLines = ItemsHandling.selectTopRankedElements(
                    testData,
                    match -> match.getMatchedImage().getPublishedName(),
                    CDMatch::getMatchingPixels,
                    -1,
                    topSubResults);
            assertEquals(testDataByLine.size(), rankedLines.size());
            rankedLines.forEach(lineMatches -> {
                assertEquals(topSubResults, lineMatches.getEntry().size());
            });
        }
    }

    @Test
    public void selectTopRankedElementsWithAllSubResults() {
        List<CDMatch<EMNeuronEntity, LMNeuronEntity>> testData = createTestData();

        Map<String, List<CDMatch<EMNeuronEntity, LMNeuronEntity>>> testDataByLine = testData.stream().collect(Collectors.groupingBy(
                m -> m.getMatchedImage().getPublishedName(),
                Collectors.toList()
        ));

        for (int i = 1; i <= 3; i++) {
            List<ScoredEntry<List<CDMatch<EMNeuronEntity, LMNeuronEntity>>>> rankedLines = ItemsHandling.selectTopRankedElements(
                    testData,
                    match -> match.getMatchedImage().getPublishedName(),
                    CDMatch::getMatchingPixels,
                    i,
                    -1);
            assertEquals(i, rankedLines.size());
            rankedLines.forEach(lineResults -> {
                assertEquals(testDataByLine.get(lineResults.getName()).size(), lineResults.getEntry().size());
                // check that the score for the group is the max score
                testDataByLine.get(lineResults.getName()).forEach(testLineMatch -> {
                    assertTrue(testLineMatch.getMatchingPixels() <= lineResults.getScore().intValue());
                });
                // check that all matches from this group are for the same line
                lineResults.getEntry().forEach(lineMatch -> {
                    assertEquals(lineResults.getName(), lineMatch.getMatchedImage().getPublishedName());
                });
            });
        }
    }

    @Test
    public void selectTopRankedElementsWithLimitedSubResults() {
        List<CDMatch<EMNeuronEntity, LMNeuronEntity>> testData = createTestData();

        Map<String, List<CDMatch<EMNeuronEntity, LMNeuronEntity>>> testDataByLine = testData.stream().collect(Collectors.groupingBy(
                m -> m.getMatchedImage().getPublishedName(),
                Collectors.toList()
        ));

        for (int i = 1; i <= 3; i++) {
            for (int si = 1; si <= 3; si++) {
                int topSubResults = si;
                List<ScoredEntry<List<CDMatch<EMNeuronEntity, LMNeuronEntity>>>> rankedLines = ItemsHandling.selectTopRankedElements(
                        testData,
                        match -> match.getMatchedImage().getPublishedName(),
                        CDMatch::getMatchingPixels,
                        i,
                        topSubResults);
                assertEquals(i, rankedLines.size());
                // check that there are no more then topSubResults selected
                rankedLines.forEach(lineResults -> {
                    assertEquals(topSubResults, lineResults.getEntry().size());
                });
            }
        }
    }

    private List<CDMatch<EMNeuronEntity, LMNeuronEntity>> createTestData() {
        return Arrays.asList(
                // matches with line l1
                createCDSMatch("l1", "s1.1", 45),
                createCDSMatch("l1", "s1.1", 44),
                createCDSMatch("l1", "s1.1", 43),
                createCDSMatch("l1", "s1.2", 35),
                createCDSMatch("l1", "s1.2", 34),
                createCDSMatch("l1", "s1.2", 33),
                createCDSMatch("l1", "s1.3", 25),
                createCDSMatch("l1", "s1.3", 24),
                createCDSMatch("l1", "s1.3", 23),
                createCDSMatch("l1", "s1.4", 15),
                createCDSMatch("l1", "s1.4", 14),
                createCDSMatch("l1", "s1.4", 13),
                // matches with line l2
                createCDSMatch("l2", "s2.1", 44),
                createCDSMatch("l2", "s2.1", 43),
                createCDSMatch("l2", "s2.1", 42),
                createCDSMatch("l2", "s2.2", 34),
                createCDSMatch("l2", "s2.2", 33),
                createCDSMatch("l2", "s2.2", 32),
                createCDSMatch("l2", "s2.3", 24),
                createCDSMatch("l2", "s2.3", 23),
                createCDSMatch("l2", "s2.3", 22),
                createCDSMatch("l2", "s2.4", 14),
                createCDSMatch("l2", "s2.4", 13),
                createCDSMatch("l2", "s2.4", 12),
                // matches with line l3
                createCDSMatch("l3", "s3.1", 43),
                createCDSMatch("l3", "s3.1", 42),
                createCDSMatch("l3", "s3.1", 41),
                createCDSMatch("l3", "s3.2", 33),
                createCDSMatch("l3", "s3.2", 32),
                createCDSMatch("l3", "s3.2", 31),
                createCDSMatch("l3", "s3.3", 23),
                createCDSMatch("l3", "s3.3", 22),
                createCDSMatch("l3", "s3.3", 21),
                createCDSMatch("l3", "s3.4", 13),
                createCDSMatch("l3", "s3.4", 12),
                createCDSMatch("l3", "s3.4", 11)
        );
    }

    private CDMatch<EMNeuronEntity, LMNeuronEntity> createCDSMatch(String line,
                                                                   String slideCode,
                                                                   int matchingPixels) {
        CDMatch<EMNeuronEntity, LMNeuronEntity> match = new CDMatch<>();
        LMNeuronEntity lmNeuronMetadata = new LMNeuronEntity();
        lmNeuronMetadata.setPublishedName(line);
        lmNeuronMetadata.setSlideCode(slideCode);
        match.setMatchedImage(lmNeuronMetadata);
        match.setMatchingPixels(matchingPixels);
        return match;
    }
}
