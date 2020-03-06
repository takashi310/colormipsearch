package org.janelia.colormipsearch;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

class MIPInfo implements Serializable {
    @JsonProperty
    String id;
    @JsonProperty
    String libraryName;
    @JsonProperty
    String publishedName;
    @JsonProperty
    String imageFilepath;
    @JsonProperty
    String cdmFilepath;
    @JsonProperty
    String imageURL;
    @JsonProperty
    String thumbnailURL;

    MIPInfo() {
    }

    MIPInfo(MIPInfo that) {
        this.id = that.id;
        this.libraryName = that.libraryName;
        this.publishedName = that.publishedName;
        this.imageFilepath = that.imageFilepath;
        this.cdmFilepath = that.cdmFilepath;
        this.imageURL = that.imageURL;
        this.thumbnailURL = that.thumbnailURL;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        MIPInfo mipImage = (MIPInfo) o;

        return new EqualsBuilder()
                .append(id, mipImage.id)
                .append(cdmFilepath, mipImage.cdmFilepath)
                .append(imageFilepath, mipImage.imageFilepath)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(id)
                .append(cdmFilepath)
                .append(imageFilepath)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("cdmFilepath", cdmFilepath)
                .append("imageFilepath", imageFilepath)
                .toString();
    }

    boolean isEmSkelotonMIP() {
        return libraryName != null && StringUtils.equalsIgnoreCase(libraryName, "flyem_hemibrain") ||
                cdmFilepath != null && (StringUtils.containsIgnoreCase(cdmFilepath, "flyem") || StringUtils.containsIgnoreCase(cdmFilepath, "hemi"));
    }
}
