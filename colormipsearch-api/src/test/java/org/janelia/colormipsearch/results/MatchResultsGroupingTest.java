package org.janelia.colormipsearch.results;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.model.EMNeuronEntity;
import org.janelia.colormipsearch.model.LMNeuronEntity;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MatchResultsGroupingTest {
    private static final String TESTCDSMATCHES_FILE= "src/test/resources/colormipsearch/results/testcdsmatches.json";

    @Test
    public void groupByMask() {
        List<CDMatch<EMNeuronEntity, LMNeuronEntity>> testMatches = readTestMatches();
        checkGroupedResults(
                MatchResultsGrouping.groupByMaskFields(
                        testMatches,
                        Collections.singletonList(
                                AbstractNeuronEntity::getMipId
                        ),
                        Comparator.comparingDouble(aCDSMatch -> Math.abs(aCDSMatch.getNormalizedScore()))),
                EMNeuronEntity.class,
                LMNeuronEntity.class
        );
    }

    @Test
    public void groupByMatched() {
        List<CDMatch<EMNeuronEntity, LMNeuronEntity>> testMatches = readTestMatches();
        checkGroupedResults(
                MatchResultsGrouping.groupByTargetFields(
                        testMatches,
                        Collections.singletonList(
                                AbstractNeuronEntity::getMipId
                        ),
                        Comparator.comparingDouble(aCDSMatch -> Math.abs(aCDSMatch.getNormalizedScore()))),
                LMNeuronEntity.class,
                EMNeuronEntity.class
        );
    }

    private <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity> void checkGroupedResults(
            List<ResultMatches<M, T, CDMatch<M, T>>> cdsResultsList,
            Class<M> expectedMaskClass,
            Class<T> expectedTargetClass) {
        assertTrue(cdsResultsList.size() > 0);
        for (ResultMatches<M, T, CDMatch<M, T>> cdsResults : cdsResultsList) {
            assertEquals(expectedMaskClass, cdsResults.getKey().getClass());
            for (CDMatch<M, T> CDMatch : cdsResults.getItems()) {
                assertNull(CDMatch.getMaskImage());
                assertNotNull(CDMatch.getMatchedImage());
                assertEquals(expectedTargetClass, CDMatch.getMatchedImage().getClass());
                assertTrue(CDMatch.getMatchFiles().size() > 0);
                assertTrue(CDMatch.getMatchComputeFiles().size() > 0);
            }
        }
    }

    @Test
    public void expandResultsGroupedByMask() {
        List<CDMatch<EMNeuronEntity, LMNeuronEntity>> testMatches = readTestMatches();
        List<ResultMatches<EMNeuronEntity, LMNeuronEntity, CDMatch<EMNeuronEntity, LMNeuronEntity>>> groupedResults = MatchResultsGrouping.groupByMaskFields(
                testMatches,
                Collections.singletonList(
                        AbstractNeuronEntity::getMipId
                ),
                Comparator.comparingDouble(aCDSMatch -> Math.abs(aCDSMatch.getNormalizedScore())));
        Comparator<CDMatch<EMNeuronEntity, LMNeuronEntity>> ordering =
                Comparator.comparing(m -> m.getMaskImage().getMipId() + m.getMatchedImage().getMipId() + m.getMatchingPixels());
        List<CDMatch<EMNeuronEntity, LMNeuronEntity>> expectedResults =
                testMatches.stream()
                        .peek(m -> { m.resetMatchFiles(); m.resetMatchComputeFiles(); })
                        .sorted(ordering)
                        .collect(Collectors.toList());
        List<CDMatch<EMNeuronEntity, LMNeuronEntity>> expandedResults =
                groupedResults.stream()
                        .map(MatchResultsGrouping::expandResultsByMask)
                        .flatMap(Collection::stream)
                        .sorted(ordering)
                        .collect(Collectors.toList());
        assertArrayEquals(expectedResults.toArray(), expandedResults.toArray());
    }

    @Test
    public void expandResultsGroupedByTarget() {
        List<CDMatch<EMNeuronEntity, LMNeuronEntity>> testMatches = readTestMatches();
        List<ResultMatches<LMNeuronEntity, EMNeuronEntity, CDMatch<LMNeuronEntity, EMNeuronEntity>>> groupedResults = MatchResultsGrouping.groupByTargetFields(
                testMatches,
                Collections.singletonList(
                        AbstractNeuronEntity::getMipId
                ),
                Comparator.comparingDouble(aCDSMatch -> Math.abs(aCDSMatch.getNormalizedScore())));
        Comparator<AbstractMatch<EMNeuronEntity, LMNeuronEntity>> ordering =
                Comparator.comparing(m -> m.getMaskImage().getMipId() + m.getMatchedImage().getMipId() + ((CDMatch<?,?>) m).getMatchingPixels());
        List<CDMatch<EMNeuronEntity, LMNeuronEntity>> expectedResults =
                testMatches.stream()
                        .peek(m -> { m.resetMatchFiles(); m.resetMatchComputeFiles(); })
                        .sorted(ordering)
                        .collect(Collectors.toList());
        List<AbstractMatch<EMNeuronEntity, LMNeuronEntity>> expandedResults =
                groupedResults.stream()
                        .map(MatchResultsGrouping::expandResultsByTarget)
                        .flatMap(Collection::stream)
                        .sorted(ordering)
                        .collect(Collectors.toList());
        assertArrayEquals(expectedResults.toArray(), expandedResults.toArray());
    }

    private List<CDMatch<EMNeuronEntity, LMNeuronEntity>> readTestMatches() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(new File(TESTCDSMATCHES_FILE), new TypeReference<List<CDMatch<EMNeuronEntity, LMNeuronEntity>>>() {
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
