package org.janelia.colormipsearch.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.janelia.colormipsearch.model.annotations.PersistenceInfo;

public class PublishedImageFields extends AbstractBaseEntity {
    private String sampleRef;
    private String line;
    private String area;
    private String tile;
    private String originalLine;
    private String slideCode;
    private String objective;
    private String alignmentSpace;
    private String releaseName;
    private Map<String, String> files = new HashMap<>();

    public String getSampleRef() {
        return sampleRef;
    }

    public void setSampleRef(String sampleRef) {
        this.sampleRef = sampleRef;
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getTile() {
        return tile;
    }

    public void setTile(String tile) {
        this.tile = tile;
    }

    public String getOriginalLine() {
        return originalLine;
    }

    public void setOriginalLine(String originalLine) {
        this.originalLine = originalLine;
    }

    public String getSlideCode() {
        return slideCode;
    }

    public void setSlideCode(String slideCode) {
        this.slideCode = slideCode;
    }

    public String getObjective() {
        return objective;
    }

    public void setObjective(String objective) {
        this.objective = objective;
    }

    public String getAlignmentSpace() {
        return alignmentSpace;
    }

    public void setAlignmentSpace(String alignmentSpace) {
        this.alignmentSpace = alignmentSpace;
    }

    public String getReleaseName() {
        return releaseName;
    }

    public void setReleaseName(String releaseName) {
        this.releaseName = releaseName;
    }

    public Map<String, String> getFiles() {
        return files;
    }

    void setFiles(Map<String, String> files) {
        this.files = files;
    }

    @JsonProperty("creationDate")
    public Date getCreatedDate() {
        return super.getCreatedDate();
    }

}
