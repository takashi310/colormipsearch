package org.janelia.colormipsearch.cmd.jacsdata;

import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * This is the JACS representation of a PublishedImage, but the only thing it is needed for is for the 3-D stack
 */
public class SamplePublishedData implements Serializable {
    @JsonProperty("_id")
    public String id;
    @JsonProperty
    public String name;
    @JsonProperty
    public String line;
    @JsonProperty
    public String area;
    @JsonProperty
    public String tile;
    @JsonProperty
    public String releaseName;
    @JsonProperty
    public String slideCode;
    @JsonProperty
    public String objective;
    @JsonProperty
    public String alignmentSpace;
    @JsonProperty
    public String sampleRef;
    @JsonProperty
    public Map<String, String> files;

    // following ColorDepthMIP, adjust as needed
    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("name", name)
                .toString();
    }
}
