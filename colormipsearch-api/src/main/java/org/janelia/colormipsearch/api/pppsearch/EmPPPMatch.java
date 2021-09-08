package org.janelia.colormipsearch.api.pppsearch;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * These are the source PPP matches as they are imported from the original matches.
 * This object contains all fields currently read from the original result file.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "id", "publishedName", "libraryName",
        "pppRank", "pppScore",
        "sampleName", "slideCode", "objective",
        "gender", "alignmentSpace", "mountingProtocol",
        "coverageScore", "aggregateCoverage", "mirrored",
        "files", "sourceImageFiles", "skeletonMatches"
})
public class EmPPPMatch extends AbstractPPPMatch {

    @JsonProperty("id")
    @Override
    public String getSampleId() {
        return super.getSampleId();
    }

    @JsonProperty("publishedName")
    @Override
    public String getLineName() {
        return super.getLineName();
    }

    @JsonProperty("libraryName")
    @Override
    public String getSourceLmDataset() {
        return super.getSourceLmDataset();
    }

    @JsonProperty
    @Override
    public String getSampleName() {
        return super.getSampleName();
    }

    @JsonProperty
    @Override
    public String getSlideCode() {
        return super.getSlideCode();
    }

    @JsonProperty
    @Override
    public String getObjective() {
        return super.getObjective();
    }

    @JsonProperty
    @Override
    public String getMountingProtocol() {
        return super.getMountingProtocol();
    }

    @JsonProperty
    @Override
    public String getGender() {
        return super.getGender();
    }

}
