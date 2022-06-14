package org.janelia.colormipsearch.cmd;

import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * This is the JACS representation of a PublishedImage, but the only thing it is needed for is for the 3-D stack
 */
class SamplePublishedData implements Serializable {
    @JsonProperty("_id")
    String id;
    @JsonProperty
    String name;
    @JsonProperty
    String line;
    @JsonProperty
    String area;
    @JsonProperty
    String tile;
    @JsonProperty
    String releaseName;
    @JsonProperty
    String slideCode;
    @JsonProperty
    String objective;
    @JsonProperty
    String alignmentSpace;
    @JsonProperty
    String sampleRef;
    @JsonProperty
    Map<String, String> files;

    // following ColorDepthMIP, adjust as needed
    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("name", name)
                .toString();
    }
}
