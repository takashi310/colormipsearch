package org.janelia.colormipsearch.cmd;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * This is the representation of a JACS Sample.
 */
class CDMIPSample {
    @JsonProperty("_id")
    String id;
    @JsonProperty
    String name;
    @JsonProperty
    String dataSet;
    @JsonProperty
    String gender;
    @JsonProperty
    String mountingProtocol;
    @JsonProperty
    String driver;
    @JsonProperty
    String organism;
    @JsonProperty
    String genotype;
    @JsonProperty
    String flycoreId;
    @JsonProperty
    String line;
    @JsonProperty
    String slideCode;
    @JsonProperty
    String publishingName;
    @JsonProperty
    Boolean publishedToStaging;
    @JsonProperty
    String publishedExternally;
    @JsonProperty
    String crossBarcode;
    @JsonProperty
    String sampleRef;
    @JsonProperty
    String status;
    @JsonProperty
    String releaseLabel;
    @JsonProperty
    List<String> publishedObjectives;

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("name", name)
                .toString();
    }
}
