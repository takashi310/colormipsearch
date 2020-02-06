package org.janelia.colormipsearch;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import ij.ImagePlus;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

class MIPImage implements Serializable {
    @JsonProperty
    String id;
    @JsonProperty
    String filepath;
    @JsonIgnore
    transient ImagePlus image;

    MIPImage withImage(ImagePlus image) {
        this.image = image;
        return this;
    }

    boolean hasNoImage() {
        return image == null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        MIPImage mipImage = (MIPImage) o;

        return new EqualsBuilder()
                .append(id, mipImage.id)
                .append(filepath, mipImage.filepath)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(id)
                .append(filepath)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("filepath", filepath)
                .append("image", image != null ? "loaded" : "not loaded")
                .toString();
    }
}
