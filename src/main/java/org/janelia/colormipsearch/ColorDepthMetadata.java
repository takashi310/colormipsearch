package org.janelia.colormipsearch;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;

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

    MIPInfo asMIPInfo() {
        MIPInfo mipInfo = new MIPInfo();
        mipInfo.id = id;
        mipInfo.libraryName = libraryName;
        mipInfo.publishedName = publishedName;
        mipInfo.type = type;
        mipInfo.archivePath = segmentedDataBasePath;
        mipInfo.imagePath = StringUtils.defaultIfBlank(segmentFilepath, filepath);
        mipInfo.cdmPath = filepath;
        mipInfo.imageURL = imageUrl;
        mipInfo.thumbnailURL = thumbnailUrl;
        mipInfo.relatedImageRefId = extractIdFromRef(sourceImageRef);
        attrs.forEach((k, v) -> mipInfo.attrs.put(k, v));
        return mipInfo;
    }

    private String extractIdFromRef(String ref) {
        if (StringUtils.isBlank(ref)) {
            return null;
        } else {
            int idseparator = ref.indexOf('#');
            if (idseparator == -1) {
                return null; // not a valid stringified reference
            } else {
                return StringUtils.defaultIfBlank(ref.substring(idseparator + 1), null);
            }
        }
    }
}
