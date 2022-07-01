package org.janelia.colormipsearch.api_v2.pppsearch;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import org.janelia.colormipsearch.model.JsonRequired;

/**
 * These are the source PPP matches as they are imported from the original matches.
 * This object contains all fields currently read from the original result file.
 */
@JsonClassDescription("Patch per Pixel Match")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "id", "publishedName", "libraryName",
        "pppRank", "pppScore",
        "sampleName", "slideCode", "anatomicalArea", "objective",
        "gender", "alignmentSpace", "mountingProtocol",
        "coverageScore", "aggregateCoverage", "mirrored",
        "files", "sourceImageFiles", "skeletonMatches"
})
public class EmPPPMatch extends AbstractPPPMatch {

    @JsonRequired
    @JsonProperty("id")
    @Override
    public String getSampleId() {
        return super.getSampleId();
    }

    @JsonRequired
    @JsonProperty("publishedName")
    @Override
    public String getLineName() {
        return super.getLineName();
    }

    @JsonRequired
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
