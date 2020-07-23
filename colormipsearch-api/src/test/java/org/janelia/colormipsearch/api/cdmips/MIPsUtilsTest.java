package org.janelia.colormipsearch.api.cdmips;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MIPsUtilsTest {

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
