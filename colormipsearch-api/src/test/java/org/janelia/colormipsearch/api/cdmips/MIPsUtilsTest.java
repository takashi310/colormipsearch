package org.janelia.colormipsearch.api.cdmips;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MIPsUtilsTest {
    private ObjectMapper mapper;

    @Before
    public void setUp() {
        mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Test
    public void loadMIPsWithVariants() {
        String[] mipFiles = new String[] {
                "src/test/resources/colormipsearch/api/cdmips/mipsWithVariants.json"
        };
        for (String mipFile : mipFiles) {
            List<MIPMetadata> mips = MIPsUtils.readMIPsFromJSON(mipFile, 0, -1, null, mapper);
            Assert.assertTrue(mips.size() > 0);
            for (MIPMetadata mip : mips) {
                Assert.assertTrue(mip.getVariantTypes().size() > 0);
                Assert.assertNotNull(mip.getSampleRef());
            }
        }
    }

    @Test
    public void ancillaryMIPsCandidates() {
        Path mipPath = Paths.get("/a/b/c/d/mip.tif");
        Path mipParentPath = mipPath.getParent();
        int nComponents = mipParentPath.getNameCount();
        Function<String, String> ancillaryMIPSuffixMapping = nc -> nc + "_suffix";
        List<String> ancillaryMIPPathCandidates = Stream.concat(
                IntStream.range(0, nComponents)
                        .map(i -> nComponents - i - 1)
                        .mapToObj(i -> {
                            if (i > 0)
                                return mipParentPath.subpath(0, i).resolve(ancillaryMIPSuffixMapping.apply(mipParentPath.getName(i).toString())).toString();
                            else
                                return ancillaryMIPSuffixMapping.apply(mipParentPath.getName(i).toString());
                        }),
                Stream.of(""))
                .collect(Collectors.toList());
        assertEquals(
                Arrays.asList("a/b/c/d_suffix", "a/b/c_suffix", "a/b_suffix", "a_suffix", ""),
                ancillaryMIPPathCandidates);
    }
}
