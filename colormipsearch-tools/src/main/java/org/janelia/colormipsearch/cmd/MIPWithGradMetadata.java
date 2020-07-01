package org.janelia.colormipsearch.cmd;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.janelia.colormipsearch.api.cdmips.MIPMetadata;

class MIPWithGradMetadata extends MIPMetadata {
    // gradient image location
    private String imageGradientArchivePath;
    private String imageGradientName;
    private String imageGradientType;
    // zgap image location
    private String imageZGapArchivePath;
    private String imageZGapName;
    private String imageZGapType;
    private String sampleRef;

    @JsonProperty
    String getImageGradientArchivePath() {
        return imageGradientArchivePath;
    }

    void setImageGradientArchivePath(String imageGradientArchivePath) {
        this.imageGradientArchivePath = imageGradientArchivePath;
    }

    @JsonProperty
    String getImageGradientName() {
        return imageGradientName;
    }

    void setImageGradientName(String imageGradientName) {
        this.imageGradientName = imageGradientName;
    }

    @JsonProperty
    String getImageGradientType() {
        return imageGradientType;
    }

    void setImageGradientType(String imageGradientType) {
        this.imageGradientType = imageGradientType;
    }

    @JsonProperty
    String getImageZGapArchivePath() {
        return imageZGapArchivePath;
    }

    void setImageZGapArchivePath(String imageZGapArchivePath) {
        this.imageZGapArchivePath = imageZGapArchivePath;
    }

    @JsonProperty
    String getImageZGapName() {
        return imageZGapName;
    }

    void setImageZGapName(String imageZGapName) {
        this.imageZGapName = imageZGapName;
    }

    @JsonProperty
    String getImageZGapType() {
        return imageZGapType;
    }

    void setImageZGapType(String imageZGapType) {
        this.imageZGapType = imageZGapType;
    }

    @JsonProperty
    public String getSampleRef() {
        return sampleRef;
    }

    public void setSampleRef(String sampleRef) {
        this.sampleRef = sampleRef;
    }
}
