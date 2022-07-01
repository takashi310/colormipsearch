package org.janelia.colormipsearch.results;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDSMatch;
import org.janelia.colormipsearch.model.EMNeuronMetadata;
import org.janelia.colormipsearch.model.LMNeuronMetadata;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MatchResultsGroupingTest {
    private static final String TESTCDSMATCHES_FILE= "src/test/resources/colormipsearch/results/testcdsmatches.json";

    @Test
    public void groupByMask() {
        List<CDSMatch<EMNeuronMetadata, LMNeuronMetadata>> testMatches = readTestMatches();
        checkGroupedResults(
                MatchResultsGrouping.groupByMaskFields(
                        testMatches,
                        Collections.singletonList(
                                AbstractNeuronMetadata::getId
                        ),
                        Comparator.comparingDouble(aCDSMatch -> Math.abs(aCDSMatch.getNormalizedScore()))),
                EMNeuronMetadata.class,
                LMNeuronMetadata.class
        );
    }

    @Test
    public void groupByMatched() {
        List<CDSMatch<EMNeuronMetadata, LMNeuronMetadata>> testMatches = readTestMatches();
        checkGroupedResults(
                MatchResultsGrouping.groupByMatchedFields(
                        testMatches,
                        Collections.singletonList(
                                AbstractNeuronMetadata::getId
                        ),
                        Comparator.comparingDouble(aCDSMatch -> Math.abs(aCDSMatch.getNormalizedScore()))),
                LMNeuronMetadata.class,
                EMNeuronMetadata.class
        );
    }

    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> void checkGroupedResults(
            List<ResultMatches<M, T, CDSMatch<M, T>>> cdsResultsList,
            Class<M> expectedMaskClass,
            Class<T> expectedTargetClass) {
        assertTrue(cdsResultsList.size() > 0);
        for (ResultMatches<M, T, CDSMatch<M, T>> cdsResults : cdsResultsList) {
            assertEquals(expectedMaskClass, cdsResults.getKey().getClass());
            for (CDSMatch<M, T> cdsMatch : cdsResults.getItems()) {
                assertNull(cdsMatch.getMaskImage());
                assertNotNull(cdsMatch.getMatchedImage());
                assertEquals(expectedTargetClass, cdsMatch.getMatchedImage().getClass());
                assertTrue(cdsMatch.getMatchFiles().size() > 0);
                assertTrue(cdsMatch.getMatchComputeFiles().size() > 0);
            }
        }
    }

    private List<CDSMatch<EMNeuronMetadata, LMNeuronMetadata>> readTestMatches() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(new File(TESTCDSMATCHES_FILE), new TypeReference<List<CDSMatch<EMNeuronMetadata, LMNeuronMetadata>>>() {
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
