package org.janelia.colormipsearch.api.pppsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.janelia.colormipsearch.api.FileType;

public class PPPImageFile {

    private final FileType imageFileType;
    private final String imagePath;

    @JsonCreator
    public PPPImageFile(@JsonProperty FileType imageFileType, @JsonProperty String imagePath) {
        this.imageFileType = imageFileType;
        this.imagePath = imagePath;
    }

    public FileType getVariantType() {
        return imageFileType;
    }

    public String getImagePath() {
        return imagePath;
    }

}
