package org.janelia.colormipsearch;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;

class ColorDepthMetadata extends MetadataAttrs {
    @JsonProperty
    String internalName;
    @JsonProperty
    String line;
    @JsonProperty
    String sampleRef;
    String filepath;
    String type;
    String segmentedDataBasePath;
    String segmentFilepath;

    void copyTo(ColorDepthMetadata that) {
        super.copyTo(that);
        that.internalName = this.internalName;
        that.line = this.line;
        that.sampleRef = this.sampleRef;
        that.filepath = this.filepath;
        that.segmentedDataBasePath = this.segmentedDataBasePath;
        that.segmentFilepath = this.segmentFilepath;
    }

    @Override
    String mapAttr(String attrName) {
        if (StringUtils.equalsIgnoreCase(attrName, "Published Name")) {
            return "PublishedName";
        } else {
            return attrName;
        }
    }
}
