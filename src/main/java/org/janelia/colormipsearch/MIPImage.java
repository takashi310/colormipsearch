package org.janelia.colormipsearch;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import ij.ImagePlus;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

class MIPImage implements Serializable {
    @JsonProperty("_id")
    String id;
    @JsonProperty
    String filepath;
    transient ImagePlus image;

    MIPImage withImage(ImagePlus image) {
        this.image = image;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("filepath", filepath)
                .toString();
    }
}
