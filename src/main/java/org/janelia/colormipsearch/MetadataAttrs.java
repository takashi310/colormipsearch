package org.janelia.colormipsearch;

import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;

abstract class MetadataAttrs {
    @JsonProperty
    String id;
    @JsonProperty
    String publishedName;
    @JsonIgnore
    Boolean publishedToStaging;
    @JsonProperty("image_path")
    String imageUrl;
    @JsonProperty("thumbnail_path")
    String thumbnailUrl;
    @JsonProperty
    String libraryName;
    @JsonProperty("attrs")
    Map<String, String> attrs = new LinkedHashMap<>();

    void addAttr(String attribute, String value) {
        if (StringUtils.isNotBlank(value)) {
            attrs.put(attribute, value);
        }
    }

    String getAttr(String attribute) {
        return attrs.get(attribute);
    }

    void removeAttr(String attribute) {
        attrs.remove(attribute);
    }

    void copyTo(MetadataAttrs that) {
        that.id = this.id;
        that.publishedName = this.publishedName;
        that.publishedToStaging = this.publishedToStaging;
        that.imageUrl = this.imageUrl;
        that.thumbnailUrl = this.thumbnailUrl;
        that.libraryName = this.libraryName;
        this.attrs.forEach((k, v) -> that.attrs.put(mapAttr(k), v));
    }

    String mapAttr(String attrName) {
        return attrName;
    }
}
