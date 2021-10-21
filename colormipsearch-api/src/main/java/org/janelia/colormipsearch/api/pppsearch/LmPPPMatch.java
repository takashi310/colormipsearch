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
        "neuronType", "neuronInstance",
        "sampleId", "sampleName", "slideCode", "anatomicalArea", "objective", "mountingProtocol", "gender",
        "alignmentSpace",
        "coverageScore", "aggregateCoverage", "mirrored",
        "files", "sourceImageFiles", "skeletonMatches"
})
public class LmPPPMatch extends AbstractPPPMatch {

    @JsonProperty("id")
    @Override
    public String getNeuronId() {
        return super.getNeuronName();
    }

    @JsonProperty("publishedName")
    @Override
    public String getNeuronName() {
        return super.getNeuronName();
    }

    @JsonProperty("libraryName")
    @Override
    public String getSourceEmDataset() {
        return super.getSourceEmDataset();
    }

    @JsonProperty
    @Override
    public String getNeuronType() {
        return super.getNeuronType();
    }

    @JsonProperty
    @Override
    public String getNeuronInstance() {
        return super.getNeuronInstance();
    }

    @JsonProperty
    @Override
    public String getSampleId() {
        return super.getSampleId();
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
