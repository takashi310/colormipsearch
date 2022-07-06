package org.janelia.colormipsearch.cmd.io;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDSMatch;
import org.janelia.colormipsearch.model.EMNeuronMetadata;
import org.janelia.colormipsearch.model.LMNeuronMetadata;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JSONReadWriteTest {
    private static final String TESTCDSMATCHES_FILE= "src/test/resources/cdsmatches/testcdsmatches.json";

    private static Path testDataDir;
    private static Path em2lmDir;
    private static Path lm2emDir;

    private JSONCDSResultsWriter<EMNeuronMetadata, LMNeuronMetadata> em2lmJsonWriter;
    private JSONFileCDMatchesReader<EMNeuronMetadata, LMNeuronMetadata> em2lmMatchesReader;

    @BeforeClass
    public static void createTestDataDir() throws IOException {
        testDataDir = Files.createTempDirectory("cdsjsontest");
        em2lmDir = testDataDir.resolve("em2lm");
        lm2emDir = testDataDir.resolve("lm2em");
    }

    @AfterClass
    public static void removeTestDataDir() throws IOException {
        FileUtils.deleteDirectory(testDataDir.toFile());
    }

    @Before
    public void setUp() {
        ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        em2lmJsonWriter = new JSONCDSResultsWriter<>(mapper.writerWithDefaultPrettyPrinter(),
                em2lmDir,
                lm2emDir
        );
        em2lmMatchesReader = new JSONFileCDMatchesReader<>(
                Collections.emptyList(),
                mapper
        );
    }

    @Test
    public void readWriteCDSResults() {
        List<CDSMatch<EMNeuronMetadata, LMNeuronMetadata>> cdsMatches = readTestMatches();
        em2lmJsonWriter.write(cdsMatches);
        checkResultFiles(cdsMatches, em2lmDir, m -> m.getMaskImage().getId());
        checkResultFiles(cdsMatches, lm2emDir, m -> m.getMatchedImage().getId());

        IOUtils.getFiles(em2lmDir.toString(), 0, -1)
                .forEach(f -> {
                    List<CDSMatch<EMNeuronMetadata, LMNeuronMetadata>>  matchesFromFile = em2lmMatchesReader.readCDMatches(f);
                    assertTrue(matchesFromFile.size() > 0);
                    String mId = FilenameUtils.getBaseName(f);
                    List<CDSMatch<EMNeuronMetadata, LMNeuronMetadata>> testMatchesWithSameMask = cdsMatches.stream()
                            .filter(cdsMatch -> cdsMatch.getMaskImage().getId().equals(mId))
                            .peek(cdsMatch -> {
                                cdsMatch.resetMatchComputeFiles();
                                cdsMatch.resetMatchFiles();
                            })
                            .collect(Collectors.toList());
                    assertEquals("Results did not match for " + mId,
                            testMatchesWithSameMask, matchesFromFile);
                });
    }

    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> void checkResultFiles(List<CDSMatch<M, T>> matches,
                                  Path resultsDir,
                                  Function<CDSMatch<?, ?>, String> fnSelector) {
        matches.stream()
                .map(fnSelector)
                .forEach(fname -> {
                    assertTrue(Files.exists(resultsDir.resolve(fname + ".json")));
                });
    }

    private List<CDSMatch<EMNeuronMetadata, LMNeuronMetadata>> readTestMatches() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            List<CDSMatch<EMNeuronMetadata, LMNeuronMetadata>> cdsMatches =
                    mapper.readValue(new File(TESTCDSMATCHES_FILE), new TypeReference<List<CDSMatch<EMNeuronMetadata, LMNeuronMetadata>>>() {});
            cdsMatches.sort((m1, m2) -> -m1.getMatchingPixels());
            return cdsMatches;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
