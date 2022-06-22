package org.janelia.colormipsearch.cmd;

import java.io.Serializable;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * This is the representation of a JACS ColorDepthMIP image.
 */
class ColorDepthMIP implements Serializable {
    @JsonProperty("_id")
    String id;
    @JsonProperty
    String name;
    @JsonProperty
    String filepath;
    @JsonProperty
    String objective;
    @JsonProperty
    String alignmentSpace;
    @JsonProperty
    String anatomicalArea;
    @JsonProperty
    String channelNumber;
    @JsonProperty
    Long bodyId;
    @JsonProperty
    String neuronType;
    @JsonProperty
    String neuronInstance;
    @JsonProperty
    String publicImageUrl;
    @JsonProperty
    String publicThumbnailUrl;
    @JsonProperty
    Set<String> libraries;
    @JsonProperty
    String sampleRef;
    @JsonProperty
    CDMIPSample sample;
    @JsonProperty
    String emBodyRef;
    @JsonProperty
    CDMIPBody emBody;
    String sample3DImageStack;
    String sampleGen1Gal4ExpressionImage;

    String findLibrary(String libraryName) {
        if (CollectionUtils.isEmpty(libraries)) {
            return null;
        } else {
            // since a MIP may be in multiple libraries we want to make sure we have the one that we requested the mip for
            return libraries.stream()
                    .filter(StringUtils::isNotBlank)
                    .filter(l -> l.equals(libraryName))
                    .findFirst().orElse(null);
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("name", name)
                .toString();
    }
}
