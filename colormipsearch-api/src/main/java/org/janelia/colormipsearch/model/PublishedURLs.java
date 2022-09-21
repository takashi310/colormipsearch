package org.janelia.colormipsearch.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.model.annotations.PersistenceInfo;

@PersistenceInfo(storeName ="publishedURL")
@JsonTypeInfo(use = JsonTypeInfo.Id.NONE)
public class PublishedURLs extends AbstractBaseEntity {

    private Map<String, String> urls = new HashMap<>();

    @JsonProperty("uploaded")
    public Map<String, String> getUrls() {
        return urls;
    }

    void setUrls(Map<String, String> urls) {
        this.urls = urls;
    }
}
