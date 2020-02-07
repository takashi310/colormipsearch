package org.janelia.colormipsearch;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

class MinimalColorDepthMIP implements Serializable {
    @JsonProperty
    String id;
    @JsonProperty
    String filepath;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        MinimalColorDepthMIP mipImage = (MinimalColorDepthMIP) o;

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
                .toString();
    }
}
