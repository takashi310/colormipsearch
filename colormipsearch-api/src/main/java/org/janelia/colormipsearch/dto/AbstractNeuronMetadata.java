package org.janelia.colormipsearch.dto;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.model.AbstractBaseEntity;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.FileData;
import org.janelia.colormipsearch.model.FileType;
import org.janelia.colormipsearch.model.Gender;
import org.janelia.colormipsearch.model.JsonRequired;
import org.janelia.colormipsearch.model.annotations.PersistenceInfo;

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
    private final Map<FileType, FileData> neuronFiles = new HashMap<>();

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
    Map<FileType, FileData> getNeuronFiles() {
        return neuronFiles;
    }

    public String getNeuronFileName(FileType t) {
        FileData f = neuronFiles.get(t);
        return f != null ? f.getName() : null;
    }

    public boolean hasNeuronFile(FileType t) {
        return neuronFiles.containsKey(t);
    }

    public FileData getNeuronFileData(FileType t) {
        return neuronFiles.get(t);
    }

    public void setNeuronFileData(FileType t, FileData fd) {
        if (fd != null) {
            neuronFiles.put(t, fd);
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
                .append(getNeuronFileData(FileType.ColorDepthMipInput), that.getNeuronFileData(FileType.ColorDepthMipInput))
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(mipId)
                .append(getNeuronFileData(FileType.ColorDepthMipInput))
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
