package org.janelia.colormipsearch;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ImageFinderTest {

    @Test
    public void findImagesWithWildCards() {
        Map<String, Integer> testData = ImmutableMap.of(
                "src/main/resources", 1,
                "src/main/res*", 1,
                "src/main/res*/*", 1,
                "src/main/res**", 1 // this glob pattern recurses into subdirs
        );
        testData.forEach((startPath, expectedResult) -> {
            List<String> testImageFiles = ImageFinder.findImages(ImmutableList.of(startPath)).collect(Collectors.toList());
            assertEquals(startPath, (int) expectedResult, testImageFiles.size());
        });
    }

}
