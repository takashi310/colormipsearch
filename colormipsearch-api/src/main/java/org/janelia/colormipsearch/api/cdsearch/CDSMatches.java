package org.janelia.colormipsearch.api.cdsearch;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.api.Results;
import org.janelia.colormipsearch.api.cdmips.MIPIdentifier;

public class CDSMatches extends Results<List<ColorMIPSearchMatchMetadata>> {

    public static CDSMatches EMPTY = new CDSMatches(
            null,
            null,
            null,
            null,
            null,
            null,
            Collections.emptyList());

    public static List<CDSMatches> fromResultsOfColorMIPSearchMatches(List<ColorMIPSearchMatchMetadata> listOfCDSMatches) {
        if (CollectionUtils.isNotEmpty(listOfCDSMatches)) {
            return listOfCDSMatches.stream()
                    .collect(Collectors.groupingBy(
                            csr -> new MIPIdentifier(
                                    csr.getSourceId(),
                                    csr.getSourcePublishedName(),
                                    csr.getSourceLibraryName(),
                                    csr.getSourceSampleRef(),
                                    csr.getSourceRelatedImageRefId(),
                                    csr.getSourceImagePath(),
                                    csr.getSourceCdmPath(),
                                    csr.getSourceImageURL()),
                            Collectors.toList()))
                    .entrySet().stream().map(e -> new CDSMatches(
                            e.getKey().getId(),
                            e.getKey().getPublishedName(),
                            e.getKey().getLibraryName(),
                            e.getKey().getSampleRef(),
                            e.getKey().getRelatedImageRefId(),
                            e.getKey().getImageURL(),
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
    private final String maskImageURL;
    private final String maskSampleRef;
    private final String maskRelatedImageRefId;

    @JsonCreator
    public static CDSMatches createCDSMatches(@JsonProperty("maskId") String maskId,
                                              @JsonProperty("maskPublishedName") String maskPublishedName,
                                              @JsonProperty("maskLibraryName") String maskLibraryName,
                                              @JsonProperty("maskSampleRef") String maskSampleRef,
                                              @JsonProperty("maskRelatedImageRefId") String maskRelatedImageRefId,
                                              @JsonProperty("maskImageURL") String maskImageURL,
                                              @JsonProperty("results") List<ColorMIPSearchMatchMetadata> results) {
        if (StringUtils.isNotBlank(maskId)) {
            results.forEach(csr -> {
                csr.setSourceId(maskId);
                csr.setSourcePublishedName(maskPublishedName);
                csr.setSourceLibraryName(maskLibraryName);
                csr.setSourceImageURL(maskImageURL);
                csr.setSourceRelatedImageRefId(maskRelatedImageRefId);
                csr.setSourceSampleRef(maskSampleRef);
            });
        }
        return new CDSMatches(
                maskId,
                maskPublishedName,
                maskLibraryName,
                maskSampleRef,
                maskRelatedImageRefId,
                maskImageURL,
                results);
    }

    CDSMatches(String maskId,
               String maskPublishedName,
               String maskLibraryName,
               String maskSampleRef,
               String maskRelatedImageRefId,
               String maskImageURL,
               List<ColorMIPSearchMatchMetadata> results) {
        super(results);
        this.maskId = maskId;
        this.maskPublishedName = maskPublishedName;
        this.maskLibraryName = maskLibraryName;
        this.maskImageURL = maskImageURL;
        this.maskSampleRef = maskSampleRef;
        this.maskRelatedImageRefId = maskRelatedImageRefId;
    }

    @JsonIgnore
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

    public String getMaskImageURL() {
        return maskImageURL;
    }

    public String getMaskSampleRef() {
        return maskSampleRef;
    }

    public String getMaskRelatedImageRefId() {
        return maskRelatedImageRefId;
    }
}
