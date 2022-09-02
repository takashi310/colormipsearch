package org.janelia.colormipsearch.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.collections4.CollectionUtils;
import org.janelia.colormipsearch.model.annotations.PersistenceInfo;

@PersistenceInfo(storeName ="publishedImage")
public class PublishedImage extends PublishedImageFields {
    private List<PublishedImageFields> gal4Expressions;

    public boolean hasGal4Expression() {
        return CollectionUtils.isNotEmpty(gal4Expressions);
    }

    @JsonIgnore
    public List<PublishedImageFields> getGal4Expressions() {
        return gal4Expressions;
    }

    @JsonProperty("gal4")
    public void setGal4Expressions(List<PublishedImageFields> gal4Expressions) {
        this.gal4Expressions = gal4Expressions;
    }
}
