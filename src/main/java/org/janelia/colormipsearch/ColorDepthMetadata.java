package org.janelia.colormipsearch;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;

class ColorDepthMetadata {
    @JsonProperty
    String id;
    @JsonProperty
    String internalName;
    @JsonProperty
    String sampleRef;
    @JsonProperty
    String publishedLineOrSkeleton;
    @JsonProperty("image_path")
    String imageUrl;
    @JsonProperty("thumbnail_path")
    String thumbnailUrl;
    String libraryName;
    @JsonProperty("attrs")
    Map<String, String> attrs = new LinkedHashMap<>();

    void addAttr(String attribute, String value) {
        if (StringUtils.isNotBlank(value)) {
            attrs.put(attribute, value);
        }
    }
}
