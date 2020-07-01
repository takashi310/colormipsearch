package org.janelia.colormipsearch.api.cdsearch;

import java.io.File;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.janelia.colormipsearch.api.Results;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api.cdmips.MIPsUtils;
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
                "src/test/resources/colormipsearch/api/cdsearch/legacylmmips.json",
                "src/test/resources/colormipsearch/api/cdsearch/legacyemmips.json"
        };
        for (String mipFile : mipFiles) {
            List<MIPMetadata> mips = MIPsUtils.readMIPsFromJSON(mipFile, 0, -1, null, mapper);
            Assert.assertTrue(mips.size() > 0);
        }
    }

    @Test
    public void loadMLegacyColorDepthSearchResults() {
        String[] resultFiles = new String[] {
                "src/test/resources/colormipsearch/api/cdsearch/legacy_2757945549444349963_cdsresult.json",
                "src/test/resources/colormipsearch/api/cdsearch/legacy_2711777212448636939_cdsresult.json"
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
                "src/test/resources/colormipsearch/api/cdsearch/legacy_2757945549444349963_cdsresult.json",
                "src/test/resources/colormipsearch/api/cdsearch/legacy_2711777212448636939_cdsresult.json"
        };
        for (String resultFile : resultFiles) {
            File legacyCDSResultsFile = new File(resultFile);
            CDSMatches resultsFileContent = ColorMIPSearchResultUtils.readCDSMatchesFromJSONFile(legacyCDSResultsFile, mapper);
            Assert.assertNull(resultsFileContent.getMaskId());
            Assert.assertNull(resultsFileContent.getMaskPublishedName());
            Assert.assertNull(resultsFileContent.getMaskLibraryName());
            CDSMatches cdsMatchesFromResults = CDSMatches.singletonfromResultsOfColorMIPSearchMatches(resultsFileContent.results);
            Assert.assertNotNull(cdsMatchesFromResults.getMaskId());
            Assert.assertNotNull(cdsMatchesFromResults.getMaskPublishedName());
            Assert.assertNotNull(cdsMatchesFromResults.getMaskLibraryName());
            try {
                String json = mapper.writer().writeValueAsString(cdsMatchesFromResults);
                CDSMatches cdsMatchesReadContent = mapper.readValue(json, new TypeReference<CDSMatches>() {
                });
                Assert.assertEquals(cdsMatchesFromResults.getMaskId(), cdsMatchesReadContent.getMaskId());
                Assert.assertEquals(cdsMatchesFromResults.getMaskPublishedName(), cdsMatchesReadContent.getMaskPublishedName());
                Assert.assertEquals(cdsMatchesFromResults.getMaskLibraryName(), cdsMatchesReadContent.getMaskLibraryName());
                Assert.assertTrue(resultsFileContent.results.size() > 0);
                Assert.assertEquals(resultsFileContent.results, cdsMatchesReadContent.results);
            } catch (JsonProcessingException e) {
                Assert.fail(e.getMessage());
            }



        }
    }
}
