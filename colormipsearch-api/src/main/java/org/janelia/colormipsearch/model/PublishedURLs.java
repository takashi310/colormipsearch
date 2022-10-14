package org.janelia.colormipsearch.model;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.janelia.colormipsearch.model.annotations.PersistenceInfo;

@PersistenceInfo(storeName ="publishedURL")
public class PublishedURLs extends AbstractPublishedURLs {
    @JsonProperty("uploaded")
    protected Map<String, String> getUrls() {
        return super.getUrls();
    }
}
