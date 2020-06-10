package org.janelia.colormipsearch.tools;

import java.io.File;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LegacyLoadersTest {
    private ObjectMapper mapper;

    @Before
    public void setUp() {
        mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Test
    public void loadMLegacyMIP() {
        String[] mipFiles = new String[] {
                "src/test/resources/colormipsearch/tools/legacylmmips.json",
                "src/test/resources/colormipsearch/tools/legacyemmips.json"
        };
        for (String mipFile : mipFiles) {
            List<MIPMetadata> mips = MIPsUtils.readMIPsFromJSON(mipFile, 0, -1, null, mapper);
            Assert.assertTrue(mips.size() > 0);
        }
    }

    @Test
    public void loadMLegacyColorDepthSearchResults() {
        String[] resultFiles = new String[] {
                "src/test/resources/colormipsearch/tools/legacy_2757945549444349963_cdsresult.json",
                "src/test/resources/colormipsearch/tools/legacy_2711777212448636939_cdsresult.json"
        };
        for (String resultFile : resultFiles) {
            File legacyCDSResultsFile = new File(resultFile);
            Results<List<ColorMIPSearchMatchMetadata>> resultsFileContent = ColorMIPSearchResultUtils.readCDSResultsFromJSONFile(legacyCDSResultsFile, mapper);
            Assert.assertTrue(resultsFileContent.results.size() > 0);
        }
    }

    @Test
    public void loadCDSMatches() {
        String[] resultFiles = new String[] {
                "src/test/resources/colormipsearch/tools/legacy_2757945549444349963_cdsresult.json",
                "src/test/resources/colormipsearch/tools/legacy_2711777212448636939_cdsresult.json"
        };
        for (String resultFile : resultFiles) {
            File legacyCDSResultsFile = new File(resultFile);
            Results<List<ColorMIPSearchMatchMetadata>> resultsFileContent = ColorMIPSearchResultUtils.readCDSResultsFromJSONFile(legacyCDSResultsFile, mapper);

            List<CDSMatches> cdsMatchesFromResults = CDSMatches.fromResultsOfColorMIPSearchMatches(resultsFileContent);
            Assert.assertEquals(1, cdsMatchesFromResults.size());

            try {
                String json = mapper.writer().writeValueAsString(cdsMatchesFromResults.get(0));
                CDSMatches cdsMatchesReadContent = mapper.readValue(json, new TypeReference<CDSMatches>() {
                });
                Assert.assertTrue(resultsFileContent.results.size() > 0);
                Assert.assertEquals(resultsFileContent.results, cdsMatchesReadContent.results);
            } catch (JsonProcessingException e) {
                Assert.fail(e.getMessage());
            }



        }
    }
}
