package org.janelia.colormipsearch;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

class ColorDepthMetadata extends MetadataAttrs {
    @JsonProperty
    String internalName;
    @JsonProperty
    String line;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    String sampleRef;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    String sourceImageRef;
    String filepath;
    String type;
    String segmentedDataBasePath;
    String segmentFilepath;

    @JsonIgnore
    void setEMSkeletonPublishedName(String publishedName) {
        this.publishedName = publishedName;
        addAttr("Body Id", publishedName);
        addAttr("PublishedName", publishedName);
    }

    @JsonIgnore
    void setLMLinePublishedName(String publishedName) {
        this.publishedName = publishedName;
        addAttr("Published Name", publishedName);
        addAttr("PublishedName", publishedName);
    }

    void copyTo(ColorDepthMetadata that) {
        super.copyTo(that);
        that.internalName = this.internalName;
        that.line = this.line;
        that.sampleRef = this.sampleRef;
        that.filepath = this.filepath;
        that.segmentedDataBasePath = this.segmentedDataBasePath;
        that.segmentFilepath = this.segmentFilepath;
    }

}
