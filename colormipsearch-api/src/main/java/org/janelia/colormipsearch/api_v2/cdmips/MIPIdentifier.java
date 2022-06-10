package org.janelia.colormipsearch.api_v2.cdmips;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class MIPIdentifier {
    private final String id;
    private final String publishedName;
    private final String libraryName;
    private final String sampleRef;
    private final String relatedImageRefId;
    private final String imagePath;
    private final String cdmPath;
    private final String imageURL;


    public MIPIdentifier(String id,
                         String publishedName,
                         String libraryName,
                         String sampleRef,
                         String relatedImageRefId,
                         String imagePath,
                         String cdmPath,
                         String imageURL) {
        this.id = id;
        this.publishedName = publishedName;
        this.libraryName = libraryName;
        this.sampleRef = sampleRef;
        this.relatedImageRefId = relatedImageRefId;
        this.imagePath = imagePath;
        this.cdmPath = cdmPath;
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

    public String getSampleRef() {
        return sampleRef;
    }

    public String getRelatedImageRefId() {
        return relatedImageRefId;
    }

    public String getImagePath() {
        return imagePath;
    }

    public String getCdmPath() {
        return cdmPath;
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
