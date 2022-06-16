package org.janelia.colormipsearch.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class AbstractNeuronMetadata {
    private String id; // MIP ID
    private String sourceFilepath; // file path of the source color depth MIP
                                   // this is not handled like any other filedata because
                                   // it's purpose is only to keep track of where the original mip was located
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

    @JsonIgnore
    public abstract String getNeuronId();

    public String getSourceFilepath() {
        return sourceFilepath;
    }

    public void setSourceFilepath(String sourceFilepath) {
        this.sourceFilepath = sourceFilepath;
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

    @JsonProperty("files")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    Map<FileType, FileData> getNeuronFiles() {
        return neuronFiles;
    }

    void setNeuronFiles(Map<FileType, FileData> neuronFiles) {
        if (neuronFiles == null) {
            this.neuronFiles.clear();
        } else {
            this.neuronFiles = neuronFiles;
        }
    }

    public Optional<FileData> getNeuronFileData(FileType t) {
        FileData f = neuronFiles.get(t);
        return f != null ? Optional.of(f) : Optional.empty();
    }

    public void setNeuronFileData(FileType t, FileData fd) {
        if (fd != null) {
            neuronFiles.put(t, fd);
        } else {
            neuronFiles.remove(t);
        }
    }

    public abstract <N extends AbstractNeuronMetadata> N duplicate();

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("libraryName", libraryName)
                .append("publishedName", publishedName)
                .toString();
    }

    protected <N extends AbstractNeuronMetadata> void copyFrom(N that) {
        this.id = that.getId();
        this.sourceFilepath = that.getSourceFilepath();
        this.libraryName = that.getLibraryName();
        this.publishedName = that.getPublishedName();
        this.alignmentSpace = that.getAlignmentSpace();
        this.gender = that.getGender();
        this.neuronFiles.putAll(that.getNeuronFiles());
    }

}
