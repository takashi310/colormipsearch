package org.janelia.colormipsearch.model;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import org.janelia.colormipsearch.model.annotations.PersistenceInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NONE)
public abstract class AbstractPublishedURLs extends AbstractBaseEntity {

    private Map<String, String> urls = new HashMap<>();

    protected Map<String, String> getUrls() {
        return urls;
    }

    protected void setUrls(Map<String, String> urls) {
        this.urls = urls;
    }

    public String getURLFor(String fileType, String defaultValue) {
        return urls.getOrDefault(fileType, defaultValue);
    }
}
