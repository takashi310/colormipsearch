package org.janelia.colormipsearch.api.cdsearch;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.janelia.colormipsearch.api.cdmips.AbstractMetadata;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;

class IDDominantMIPMetadata extends MIPMetadata {
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        AbstractMetadata mipMetadata = (AbstractMetadata) o;

        return new EqualsBuilder()
                .append(getId(), mipMetadata.getId())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(getId())
                .toHashCode();
    }
}
