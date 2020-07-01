package org.janelia.colormipsearch.cmd;

import java.nio.file.Paths;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.tools.AbstractMetadata;
import org.janelia.colormipsearch.tools.MIPMetadata;

class ColorDepthMetadata extends AbstractMetadata {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty
    String sourceImageRef;
    String filepath;
    String segmentedDataBasePath;
    String segmentFilepath;
    String sampleRef;

    @JsonIgnore
    void setEMSkeletonPublishedName(String publishedName) {
        this.setPublishedName(publishedName);
    }

    @JsonIgnore
    void setLMLinePublishedName(String publishedName) {
        this.setPublishedName(publishedName);
    }

    @JsonIgnore
    String getCdmName() {
        if (StringUtils.isNotBlank(filepath)) {
            return Paths.get(filepath).getFileName().toString();
        } else {
            return null;
        }
    }

    void copyTo(ColorDepthMetadata that) {
        super.copyTo(that);
        that.filepath = this.filepath;
        that.segmentedDataBasePath = this.segmentedDataBasePath;
        that.segmentFilepath = this.segmentFilepath;
        that.sampleRef = this.sampleRef;
    }

    MIPWithGradMetadata asMIPWithGradient() {
        MIPWithGradMetadata mipInfo = new MIPWithGradMetadata();
        this.copyTo(mipInfo);
        mipInfo.setImageType(this.getImageType());
        mipInfo.setImageArchivePath(segmentedDataBasePath);
        mipInfo.setImageName(StringUtils.defaultIfBlank(segmentFilepath, filepath));
        mipInfo.setCdmPath(filepath);
        mipInfo.setImageURL(getImageURL());
        mipInfo.setThumbnailURL(getThumbnailURL());
        mipInfo.setRelatedImageRefId(extractIdFromRef(sourceImageRef));
        mipInfo.setSampleRef(sampleRef);
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
