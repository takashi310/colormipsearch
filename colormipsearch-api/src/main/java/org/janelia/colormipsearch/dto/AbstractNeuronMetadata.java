package org.janelia.colormipsearch.dto;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.model.FileType;
import org.janelia.colormipsearch.model.Gender;
import org.janelia.colormipsearch.model.JsonRequired;

/**
 * This is the representation of the neuron metadata that will get uploaded to AWS
 * and then used by the NeuronBridge client application.
 */
public abstract class AbstractNeuronMetadata {

    private String mipId; // MIP ID - not to be confused with the entityId which is the primary key of this entity
    private String libraryName; // MIP library
    private String publishedName;
    private String alignmentSpace;
    private String anatomicalArea;
    private Gender gender;
    // neuronFiles holds S3 files used by the NeuronBridge app
    private final Map<FileType, String> neuronFiles = new HashMap<>();

    @JsonProperty("id")
    public String getMipId() {
        return mipId;
    }

    public void setMipId(String mipId) {
        this.mipId = mipId;
    }

    public boolean hasMipID() {
        return StringUtils.isNotBlank(mipId);
    }

    @JsonRequired
    public String getLibraryName() {
        return libraryName;
    }

    public void setLibraryName(String libraryName) {
        this.libraryName = libraryName;
    }

    @JsonRequired
    public String getPublishedName() {
        return publishedName;
    }

    public void setPublishedName(String publishedName) {
        this.publishedName = publishedName;
    }

    @JsonRequired
    public String getAlignmentSpace() {
        return alignmentSpace;
    }

    public void setAlignmentSpace(String alignmentSpace) {
        this.alignmentSpace = alignmentSpace;
    }

    public String getAnatomicalArea() {
        return anatomicalArea;
    }

    public void setAnatomicalArea(String anatomicalArea) {
        this.anatomicalArea = anatomicalArea;
    }

    public Gender getGender() {
        return gender;
    }

    public void setGender(Gender gender) {
        this.gender = gender;
    }

    @JsonProperty("files")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    Map<FileType, String> getNeuronFiles() {
        return neuronFiles;
    }

    void setNeuronFiles(Map<FileType, String> neuronFiles) {
        if (neuronFiles != null) {
            this.neuronFiles.putAll(neuronFiles);
        }
    }

    public boolean hasNeuronFile(FileType t) {
        return neuronFiles.containsKey(t);
    }

    public String getNeuronFile(FileType t) {
        return neuronFiles.get(t);
    }

    public void setNeuronFile(FileType t, String fn) {
        if (StringUtils.isNotBlank(fn)) {
            neuronFiles.put(t, fn);
        } else {
            neuronFiles.remove(t);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        AbstractNeuronMetadata that = (AbstractNeuronMetadata) o;

        return new EqualsBuilder()
                .appendSuper(super.equals(o))
                .append(mipId, that.mipId)
                .append(getNeuronFile(FileType.ColorDepthMipInput), that.getNeuronFile(FileType.ColorDepthMipInput))
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(mipId)
                .append(getNeuronFile(FileType.ColorDepthMipInput))
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("mipId", mipId)
                .append("libraryName", libraryName)
                .append("publishedName", publishedName)
                .toString();
    }

    protected <N extends AbstractNeuronMetadata> void copyFrom(N that) {
        this.mipId = that.getMipId();
        this.libraryName = that.getLibraryName();
        this.publishedName = that.getPublishedName();
        this.alignmentSpace = that.getAlignmentSpace();
        this.gender = that.getGender();
        this.neuronFiles.clear();
        this.neuronFiles.putAll(that.getNeuronFiles());
    }

}
