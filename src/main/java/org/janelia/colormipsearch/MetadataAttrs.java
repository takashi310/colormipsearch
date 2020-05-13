package org.janelia.colormipsearch;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

public abstract class MetadataAttrs {
    private String id;
    private String publishedName;
    private Boolean publishedToStaging;
    private String imageUrl;
    private String thumbnailUrl;
    private String libraryName;
    @JsonProperty("attrs")
    private Map<String, String> attrs = new LinkedHashMap<>();

    @JsonProperty
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @JsonProperty
    public String getPublishedName() {
        return publishedName;
    }

    public void setPublishedName(String publishedName) {
        this.publishedName = publishedName;
    }

    @JsonIgnore
    public Boolean getPublishedToStaging() {
        return publishedToStaging;
    }

    public void setPublishedToStaging(Boolean publishedToStaging) {
        this.publishedToStaging = publishedToStaging;
    }

    @JsonProperty("image_path")
    public String getImageUrl() {
        return imageUrl;
    }

    public void setImageUrl(String imageUrl) {
        this.imageUrl = imageUrl;
    }

    @JsonProperty("thumbnail_path")
    public String getThumbnailUrl() {
        return thumbnailUrl;
    }

    public void setThumbnailUrl(String thumbnailUrl) {
        this.thumbnailUrl = thumbnailUrl;
    }

    @JsonProperty
    public String getLibraryName() {
        return libraryName;
    }

    public void setLibraryName(String libraryName) {
        this.libraryName = libraryName;
    }

    public void addAttr(String attribute, String value) {
        if (StringUtils.isNotBlank(value)) {
            attrs.put(attribute, value);
        }
    }

    public String getAttr(String attribute) {
        return attrs.get(attribute);
    }

    void removeAttr(String attribute) {
        attrs.remove(attribute);
    }

    public void copyTo(MetadataAttrs that) {
        that.id = this.id;
        that.publishedName = this.publishedName;
        that.publishedToStaging = this.publishedToStaging;
        that.imageUrl = this.imageUrl;
        that.thumbnailUrl = this.thumbnailUrl;
        that.libraryName = this.libraryName;
        iterateAttrs((k, v) -> {
            that.addAttr(mapAttr(k), v);
        });
    }

    protected void iterateAttrs(BiConsumer<String, String> attrConsumer) {
        this.attrs.forEach(attrConsumer);
    }

    String mapAttr(String attrName) {
        return attrName;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("publishedName", publishedName)
                .append("libraryName", libraryName)
                .toString();
    }
}
