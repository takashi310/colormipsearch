package org.janelia.colormipsearch.api.pppsearch;

import java.io.File;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RawPPPMatchesReaderTest {

    private RawPPPMatchesReader rawPPPMatchesReader;

    @Before
    public void setUp() {
        rawPPPMatchesReader = new RawPPPMatchesReader();
    }

    @Test
    public void readRawPPPMatchFile() {
        String[] testFiles = new String[] {
                "src/test/resources/colormipsearch/api/pppsearch/cov_scores_1599747200-PFNp_c-RT_18U.json",
                "src/test/resources/colormipsearch/api/pppsearch/cov_scores_484130600-SMP145-RT_18U.json"
        };
        for (String testFile : testFiles) {
            List<PPPMatch> pppMatchList = rawPPPMatchesReader.readPPPMatches(testFile);
            assertTrue(pppMatchList.size() > 0);

            String testNeuron = new File(testFile).getName()
                    .replaceAll("\\.json", "")
                    .replaceAll("cov_scores_", "");

            pppMatchList.forEach(pppMatch -> {
                assertEquals(testFile, testNeuron, pppMatch.getFullEmName());
                assertNotNull(testFile, pppMatch.getFullLmName());
            });
        }
    }
}
