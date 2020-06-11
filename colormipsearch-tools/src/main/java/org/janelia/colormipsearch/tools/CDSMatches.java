package org.janelia.colormipsearch.tools;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class CDSMatches extends Results<List<ColorMIPSearchMatchMetadata>> {

    public static List<CDSMatches> fromResultsOfColorMIPSearchMatches(List<ColorMIPSearchMatchMetadata> listOfCDSMatches) {
        if (CollectionUtils.isNotEmpty(listOfCDSMatches)) {
            return listOfCDSMatches.stream()
                    .collect(Collectors.groupingBy(
                            csr -> new MIPIdentifier(csr.getSourceId(), csr.getSourcePublishedName(), csr.getSourceLibraryName()),
                            Collectors.toList()))
                    .entrySet().stream().map(e -> new CDSMatches(
                            e.getKey().getId(),
                            e.getKey().getPublishedName(),
                            e.getKey().getLibraryName(),
                            e.getValue()))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    public static CDSMatches singletonfromResultsOfColorMIPSearchMatches(List<ColorMIPSearchMatchMetadata> listOfCDSMatches) {
        List<CDSMatches> cdsMatches = fromResultsOfColorMIPSearchMatches(listOfCDSMatches);
        if (cdsMatches.isEmpty()) {
            throw new IllegalArgumentException("Expected a single set of color depth matches but found none");
        } else if (cdsMatches.size() > 1) {
            throw new IllegalArgumentException("Expected a single set of color depth matches but found " + cdsMatches.size());
        }
        return cdsMatches.get(0);
    }

    private final String maskId;
    private final String maskPublishedName;
    private final String maskLibraryName;

    @JsonCreator
    public CDSMatches(
            @JsonProperty("maskId") String maskId,
            @JsonProperty("maskPublishedName") String maskPublishedName,
            @JsonProperty("maskLibraryName") String maskLibraryName,
            @JsonProperty("results") List<ColorMIPSearchMatchMetadata> results) {
        super(results);
        this.maskId = maskId;
        this.maskPublishedName = maskPublishedName;
        this.maskLibraryName = maskLibraryName;

        if (StringUtils.isNotBlank(maskId)) {
            results.forEach(csr -> {
                csr.setSourceId(this.maskId);
                csr.setSourcePublishedName(this.maskPublishedName);
                csr.setSourceLibraryName(this.maskLibraryName);
            });
        }
    }

    public boolean isEmpty() {
        return CollectionUtils.isEmpty(results);
    }

    public String getMaskId() {
        return maskId;
    }

    public String getMaskPublishedName() {
        return maskPublishedName;
    }

    public String getMaskLibraryName() {
        return maskLibraryName;
    }
}
