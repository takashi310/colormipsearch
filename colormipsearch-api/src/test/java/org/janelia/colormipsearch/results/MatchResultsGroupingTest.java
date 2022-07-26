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
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.model.EMNeuronMetadata;
import org.janelia.colormipsearch.model.LMNeuronMetadata;
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
        List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> testMatches = readTestMatches();
        checkGroupedResults(
                MatchResultsGrouping.groupByMaskFields(
                        testMatches,
                        Collections.singletonList(
                                AbstractNeuronMetadata::getMipId
                        ),
                        Comparator.comparingDouble(aCDSMatch -> Math.abs(aCDSMatch.getNormalizedScore()))),
                EMNeuronMetadata.class,
                LMNeuronMetadata.class
        );
    }

    @Test
    public void groupByMatched() {
        List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> testMatches = readTestMatches();
        checkGroupedResults(
                MatchResultsGrouping.groupByTargetFields(
                        testMatches,
                        Collections.singletonList(
                                AbstractNeuronMetadata::getMipId
                        ),
                        Comparator.comparingDouble(aCDSMatch -> Math.abs(aCDSMatch.getNormalizedScore()))),
                LMNeuronMetadata.class,
                EMNeuronMetadata.class
        );
    }

    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> void checkGroupedResults(
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
        List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> testMatches = readTestMatches();
        List<ResultMatches<EMNeuronMetadata, LMNeuronMetadata, CDMatch<EMNeuronMetadata, LMNeuronMetadata>>> groupedResults = MatchResultsGrouping.groupByMaskFields(
                testMatches,
                Collections.singletonList(
                        AbstractNeuronMetadata::getMipId
                ),
                Comparator.comparingDouble(aCDSMatch -> Math.abs(aCDSMatch.getNormalizedScore())));
        Comparator<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> ordering =
                Comparator.comparing(m -> m.getMaskImage().getMipId() + m.getMatchedImage().getMipId() + m.getMatchingPixels());
        List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> expectedResults =
                testMatches.stream()
                        .peek(m -> { m.resetMatchFiles(); m.resetMatchComputeFiles(); })
                        .sorted(ordering)
                        .collect(Collectors.toList());
        List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> expandedResults =
                groupedResults.stream()
                        .map(MatchResultsGrouping::expandResultsByMask)
                        .flatMap(Collection::stream)
                        .sorted(ordering)
                        .collect(Collectors.toList());
        assertArrayEquals(expectedResults.toArray(), expandedResults.toArray());
    }

    @Test
    public void expandResultsGroupedByTarget() {
        List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> testMatches = readTestMatches();
        List<ResultMatches<LMNeuronMetadata, EMNeuronMetadata, CDMatch<LMNeuronMetadata, EMNeuronMetadata>>> groupedResults = MatchResultsGrouping.groupByTargetFields(
                testMatches,
                Collections.singletonList(
                        AbstractNeuronMetadata::getMipId
                ),
                Comparator.comparingDouble(aCDSMatch -> Math.abs(aCDSMatch.getNormalizedScore())));
        Comparator<AbstractMatch<EMNeuronMetadata, LMNeuronMetadata>> ordering =
                Comparator.comparing(m -> m.getMaskImage().getMipId() + m.getMatchedImage().getMipId() + ((CDMatch<?,?>) m).getMatchingPixels());
        List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> expectedResults =
                testMatches.stream()
                        .peek(m -> { m.resetMatchFiles(); m.resetMatchComputeFiles(); })
                        .sorted(ordering)
                        .collect(Collectors.toList());
        List<AbstractMatch<EMNeuronMetadata, LMNeuronMetadata>> expandedResults =
                groupedResults.stream()
                        .map(MatchResultsGrouping::expandResultsByTarget)
                        .flatMap(Collection::stream)
                        .sorted(ordering)
                        .collect(Collectors.toList());
        assertArrayEquals(expectedResults.toArray(), expandedResults.toArray());
    }

    private List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> readTestMatches() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(new File(TESTCDSMATCHES_FILE), new TypeReference<List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>>>() {
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
