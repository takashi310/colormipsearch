package org.janelia.colormipsearch.cmd;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.MIPInfo;
import org.janelia.colormipsearch.MetadataAttrs;

class ColorDepthMetadata extends MetadataAttrs {
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
        this.setPublishedName(publishedName);
        addAttr("Body Id", publishedName);
        addAttr("PublishedName", publishedName);
    }

    @JsonIgnore
    void setLMLinePublishedName(String publishedName) {
        this.setPublishedName(publishedName);
        addAttr("Published Name", publishedName);
        addAttr("PublishedName", publishedName);
    }

    void copyTo(ColorDepthMetadata that) {
        super.copyTo(that);
        that.sampleRef = this.sampleRef;
        that.filepath = this.filepath;
        that.segmentedDataBasePath = this.segmentedDataBasePath;
        that.segmentFilepath = this.segmentFilepath;
    }

    MIPInfo asMIPInfo() {
        MIPInfo mipInfo = new MIPInfo();
        mipInfo.setId(getId());
        mipInfo.setLibraryName(getLibraryName());
        mipInfo.setPublishedName(getPublishedName());
        mipInfo.setType(type);
        mipInfo.setArchivePath(segmentedDataBasePath);
        mipInfo.setImagePath(StringUtils.defaultIfBlank(segmentFilepath, filepath));
        mipInfo.setCdmPath(filepath);
        mipInfo.setImageURL(getImageUrl());
        mipInfo.setThumbnailURL(getThumbnailUrl());
        mipInfo.setRelatedImageRefId(extractIdFromRef(sourceImageRef));
        iterateAttrs((k, v) -> mipInfo.addAttr(k, v));
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
