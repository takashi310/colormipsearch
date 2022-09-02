package org.janelia.colormipsearch.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.janelia.colormipsearch.model.annotations.PersistenceInfo;

@PersistenceInfo(storeName ="publishedImage")
public class PublishedImage extends PublishedImageFields {
    private List<PublishedImageFields> gal4Image;

    @JsonIgnore
    public List<PublishedImageFields> getGal4Image() {
        return gal4Image;
    }

    @JsonProperty("gal4")
    public void setGal4Image(List<PublishedImageFields> gal4Image) {
        this.gal4Image = gal4Image;
    }
}
