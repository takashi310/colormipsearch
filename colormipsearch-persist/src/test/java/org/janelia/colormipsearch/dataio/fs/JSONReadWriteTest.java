package org.janelia.colormipsearch.dataio.fs;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.model.EMNeuronMetadata;
import org.janelia.colormipsearch.model.LMNeuronMetadata;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class JSONReadWriteTest {
    private static final String TESTCDSMATCHES_FILE= "src/test/resources/cdsmatches/testcdsmatches.json";

    private static Path testDataDir;
    private static Path em2lmDir;
    private static Path lm2emDir;

    private ObjectMapper mapper;
    private JSONCDSMatchesWriter<EMNeuronMetadata, LMNeuronMetadata> em2lmJsonWriter;
    private JSONCDSMatchesReader<EMNeuronMetadata, LMNeuronMetadata> em2lmMatchesReader;

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
        mapper = new ObjectMapper();
        em2lmJsonWriter = new JSONCDSMatchesWriter<>(mapper.writerWithDefaultPrettyPrinter(),
                em2lmDir,
                lm2emDir
        );
        em2lmMatchesReader = new JSONCDSMatchesReader<>(
                mapper
        );
    }

    @Test
    public void jsonCDSMatchSerialization() {
        Path testJsonOutput = testDataDir.resolve("testcdsout.json");
        List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> CDMatches = readTestMatches(new File(TESTCDSMATCHES_FILE));
        JsonOutputHelper.writeToJSONFile(CDMatches, testJsonOutput.toFile(), mapper.writerWithDefaultPrettyPrinter());
        List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> readCDMatches = readTestMatches(testJsonOutput.toFile());
        assertNotNull(readCDMatches);
        assertEquals(CDMatches, readCDMatches);
    }

    @Test
    public void readWriteCDSResults() {
        List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> CDMatches = readTestMatches(new File(TESTCDSMATCHES_FILE));
        em2lmJsonWriter.write(CDMatches);
        checkResultFiles(CDMatches, em2lmDir, m -> m.getMaskImage().getId());
        checkResultFiles(CDMatches, lm2emDir, m -> m.getMatchedImage().getId());

        FSUtils.getFiles(em2lmDir.toString(), 0, -1)
                .forEach(f -> {
                    List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>>  matchesFromFile = em2lmMatchesReader.readMatches(f);
                    assertTrue(matchesFromFile.size() > 0);
                    String mId = FilenameUtils.getBaseName(f);
                    List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> testMatchesWithSameMask = CDMatches.stream()
                            .filter(cdsMatch -> cdsMatch.getMaskImage().getId().equals(mId))
                            .peek(cdsMatch -> {
                                cdsMatch.resetMatchComputeFiles();
                                cdsMatch.resetMatchFiles();
                            })
                            .sorted(Comparator.comparingDouble(m -> -m.getMatchingPixels()))
                            .collect(Collectors.toList());
                    assertEquals("Results did not match for " + mId,
                            testMatchesWithSameMask, matchesFromFile);
                });
    }

    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> void checkResultFiles(List<CDMatch<M, T>> matches,
                                  Path resultsDir,
                                  Function<CDMatch<?, ?>, String> fnSelector) {
        matches.stream()
                .map(fnSelector)
                .forEach(fname -> {
                    assertTrue(Files.exists(resultsDir.resolve(fname + ".json")));
                });
    }

    private List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> readTestMatches(File f) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> CDMatches =
                    mapper.readValue(f, new TypeReference<List<CDMatch<EMNeuronMetadata, LMNeuronMetadata>>>() {});
            CDMatches.sort(Comparator.comparingDouble(m -> -m.getMatchingPixels()));
            return CDMatches;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
