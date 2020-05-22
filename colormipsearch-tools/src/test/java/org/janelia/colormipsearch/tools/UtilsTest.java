package org.janelia.colormipsearch.tools;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UtilsTest {

    @Test
    public void pickBestResultsWithAllSubResults() {
        List<ColorMIPSearchResultMetadata> testData = createTestData();
        for (int i = 1; i <= 3; i++) {
            List<ColorMIPSearchResultMetadata> selectedEntries = Utils.pickBestMatches(
                    testData,
                    csr -> csr.getMatchedPublishedName(),
                    ColorMIPSearchResultMetadata::getMatchingPixels,
                    i,
                    -1).stream()
                    .flatMap(se -> se.getEntry().stream())
                    .collect(Collectors.toList());
            assertEquals(3 * i, selectedEntries.size());
        }
    }

    @Test
    public void pickBestResultsWithSubResults() {
        List<ColorMIPSearchResultMetadata> testData = createTestData();
        for (int i = 1; i <= 3; i++) {
            for (int si = 1; si <= 3; si++) {
                int topSubResults = si;
                List<ColorMIPSearchResultMetadata> selectedEntries = Utils.pickBestMatches(
                        testData,
                        ColorMIPSearchResultMetadata::getMatchedPublishedName,
                        ColorMIPSearchResultMetadata::getMatchingPixels,
                        i,
                        -1).stream()
                        .flatMap(se -> Utils.pickBestMatches(
                                se.getEntry(),
                                csr -> csr.getAttr("Slide Code"), // pick best results by sample (identified by slide code)
                                ColorMIPSearchResultMetadata::getMatchingPixels,
                                topSubResults,
                                -1).stream())
                        .flatMap(se -> se.getEntry().stream())
                        .collect(Collectors.toList());
                assertEquals(i * si, selectedEntries.size());
            }
        }
    }

    @Test
    public void pickBestResultsWithLimitedSubResults() {
        List<ColorMIPSearchResultMetadata> testData = createTestData();
        for (int i = 1; i <= 3; i++) {
            for (int si = 1; si <= 3; si++) {
                int topSubResults = si;
                List<ColorMIPSearchResultMetadata> selectedEntries = Utils.pickBestMatches(
                        testData,
                        ColorMIPSearchResultMetadata::getMatchedPublishedName,
                        ColorMIPSearchResultMetadata::getMatchingPixels,
                        i,
                        1).stream()
                        .flatMap(se -> Utils.pickBestMatches(
                                se.getEntry(),
                                csr -> csr.getAttr("Slide Code"), // pick best results by sample (identified by slide code)
                                ColorMIPSearchResultMetadata::getMatchingPixels,
                                topSubResults,
                                -1).stream())
                        .flatMap(se -> se.getEntry().stream())
                        .collect(Collectors.toList());
                assertEquals(i, selectedEntries.size());
            }
        }
    }

    @Test
    public void eliminateDuplicateResults() {
        List<ColorMIPSearchResultMetadata> testData = ImmutableList.<ColorMIPSearchResultMetadata>builder()
                .add(createCSRWithMatchedIds("1", "10", "i1.1", "i10", 10))
                .add(createCSRWithMatchedIds("1", "10", "i1.2", "i10", 10))
                .add(createCSRWithMatchedIds("1", "20", "i1.1", "i20", 10))
                .add(createCSRWithMatchedIds("1", "30", "i1.1", "i30", 10))
                .add(createCSRWithMatchedIds("1", "30", "i1.2", "i30", 10))
                .build();
        List<ColorMIPSearchResultMetadata> resultsWithNoDuplicates = Utils.pickBestMatches(
                testData,
                ColorMIPSearchResultMetadata::getMatchedId,
                ColorMIPSearchResultMetadata::getMatchingPixels,
                -1,
                1)
                .stream()
                .flatMap(se -> se.getEntry().stream()).collect(Collectors.toList());
        assertEquals(3, resultsWithNoDuplicates.size());
    }

    private List<ColorMIPSearchResultMetadata> createTestData() {
        return ImmutableList.<ColorMIPSearchResultMetadata>builder()
                .add(createCSR("l1", "s1.1", 1))
                .add(createCSR("l1", "s1.2", 2))
                .add(createCSR("l1", "s1.3", 3))
                .add(createCSR("l2", "s2.1", 10))
                .add(createCSR("l2", "s2.2", 20))
                .add(createCSR("l2", "s2.3", 30))
                .add(createCSR("l3", "s3.1", 100))
                .add(createCSR("l3", "s3.2", 200))
                .add(createCSR("l3", "s3.3", 300))
                .build();
    }

    ColorMIPSearchResultMetadata createCSR(String line, String sample, int matchingPixels) {
        ColorMIPSearchResultMetadata csr = new ColorMIPSearchResultMetadata();
        csr.setMatchedPublishedName(line);
        csr.addAttr("Slide Code", sample);
        csr.setMatchingPixels(matchingPixels);
        return csr;
    }

    ColorMIPSearchResultMetadata createCSRWithMatchedIds(String id, String matchedId, String imageName, String matchedImageName, int matchingPixels) {
        ColorMIPSearchResultMetadata csr = new ColorMIPSearchResultMetadata();
        csr.setId(id);
        csr.setImageName(imageName);
        csr.setMatchedId(matchedId);
        csr.setMatchedImageName(matchedImageName);
        csr.setMatchingPixels(matchingPixels);
        return csr;
    }

}
