package org.janelia.colormipsearch.model;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.janelia.colormipsearch.model.annotations.PersistenceInfo;

@PersistenceInfo(storeName ="pppmURL")
public class PPPmURLs extends AbstractPublishedURLs {

    private Map<String, String> thumbnailUrls = new HashMap<>();

    @JsonProperty("uploadedFiles")
    protected Map<String, String> getUrls() {
        return super.getUrls();
    }

    protected Map<String, String> getThumbnailUrls() {
        return thumbnailUrls;
    }

    @JsonProperty("uploadedThumbnails")
    protected void setThumbnailUrls(Map<String, String> thumbnailUrls) {
        this.thumbnailUrls = thumbnailUrls;
    }

    public String getThumbnailURLFor(String fileType) {
        return thumbnailUrls.get(fileType);
    }

}
