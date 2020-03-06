package org.janelia.colormipsearch;

import com.fasterxml.jackson.annotation.JsonProperty;

class ColorDepthMetadata extends MetadataAttrs {
    @JsonProperty
    String internalName;
    @JsonProperty
    String line;
    @JsonProperty
    String sampleRef;
    String filepath;
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

}
