package org.janelia.colormipsearch.api.cdmips;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class MIPIdentifier {
    private final String id;
    private final String publishedName;
    private final String libraryName;
    private final String imageURL;

    public MIPIdentifier(String id, String publishedName, String libraryName, String imageURL) {
        this.id = id;
        this.publishedName = publishedName;
        this.libraryName = libraryName;
        this.imageURL = imageURL;
    }

    public String getId() {
        return id;
    }

    public String getPublishedName() {
        return publishedName;
    }

    public String getLibraryName() {
        return libraryName;
    }

    public String getImageURL() {
        return imageURL;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        MIPIdentifier that = (MIPIdentifier) o;

        return new EqualsBuilder()
                .append(id, that.id)
                .append(publishedName, that.publishedName)
                .append(libraryName, that.libraryName)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(id)
                .append(publishedName)
                .append(libraryName)
                .toHashCode();
    }
}
