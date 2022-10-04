package org.janelia.colormipsearch.cmd.dataexport;

import java.util.List;
import java.util.Objects;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class ImageStoreKey {
    public static ImageStoreKey fromList(List<String> listValues) {
        if (CollectionUtils.isEmpty(listValues)) {
            throw new IllegalArgumentException("Cannot create an image store key from an empty list");
        }
        ImageStoreKey imageStoreKey = new ImageStoreKey(listValues.get(0));
        if (listValues.size() > 1) {
            imageStoreKey.libraryName = listValues.get(1);
        }
        return imageStoreKey;
    }

    private final String alignmentSpace;
    private String libraryName;

    public ImageStoreKey(String alignmentSpace) {
        this.alignmentSpace = alignmentSpace;
    }

    ImageStoreKey(String alignmentSpace, String libraryName) {
        this.alignmentSpace = alignmentSpace;
        this.libraryName = libraryName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        ImageStoreKey that = (ImageStoreKey) o;

        return new EqualsBuilder().append(alignmentSpace, that.alignmentSpace).append(libraryName, that.libraryName).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(alignmentSpace).append(libraryName).toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("alignmentSpace", alignmentSpace)
                .append("libraryName", libraryName)
                .toString();
    }
}
