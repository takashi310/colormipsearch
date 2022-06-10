package org.janelia.colormipsearch.model;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;

public abstract class AbstractNeuronImage {
    private String id; // MIP ID
    private String libraryName; // MIP library
    private String publishedName;
    private String alignmentSpace;
    private Gender gender;
    private Map<FileType, FileData> neuronFiles = new HashMap<>();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLibraryName() {
        return libraryName;
    }

    public void setLibraryName(String libraryName) {
        this.libraryName = libraryName;
    }

    public String getPublishedName() {
        return publishedName;
    }

    public void setPublishedName(String publishedName) {
        this.publishedName = publishedName;
    }

    public String getAlignmentSpace() {
        return alignmentSpace;
    }

    public void setAlignmentSpace(String alignmentSpace) {
        this.alignmentSpace = alignmentSpace;
    }

    public Gender getGender() {
        return gender;
    }

    public void setGender(Gender gender) {
        this.gender = gender;
    }

    public Map<FileType, FileData> getNeuronFiles() {
        return neuronFiles;
    }

    public void setNeuronFiles(Map<FileType, FileData> neuronFiles) {
        this.neuronFiles = neuronFiles;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("libraryName", libraryName)
                .append("publishedName", publishedName)
                .toString();
    }
}
