package org.janelia.colormipsearch.dto;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.FileType;
import org.janelia.colormipsearch.model.Gender;
import org.janelia.colormipsearch.model.JsonRequired;
import org.janelia.colormipsearch.model.ProcessingType;

/**
 * This is the representation of the neuron metadata that will get uploaded to AWS
 * and then used by the NeuronBridge client application.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public abstract class AbstractNeuronMetadata {

    private String mipId; // MIP ID - not to be confused with the entityId which is the primary key of this entity
    private String libraryName; // MIP library
    private String publishedName;
    private String alignmentSpace;
    private String anatomicalArea;
    private Gender gender;
    private boolean unpublished;
    // neuronFiles holds S3 files used by the NeuronBridge app
    private final Map<FileType, String> neuronFiles = new HashMap<>();
    @JsonIgnore
    // neuronComputeFiles are needed to temporarily hold the files that were actually matched
    // in order to be able to generate the corresponding input name as it is on S3
    private final Map<ComputeFileType, String> neuronComputeFiles = new HashMap<>();
    @JsonIgnore
    private final Map<ProcessingType, Set<String>> processedTags = new HashMap<>();

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

    @JsonIgnore
    public boolean isUnpublished() {
        return unpublished;
    }

    public void setUnpublished(boolean unpublished) {
        this.unpublished = unpublished;
    }

    @JsonIgnore
    public boolean isPublished() {
        return !unpublished;
    }

    @JsonProperty("files")
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

    public void updateAllNeuronFiles(Function<String, String> fileNameMap) {
        Set<FileType> neuronFileTypes = neuronFiles.keySet();
        neuronFileTypes.forEach(
                ft -> setNeuronFile(ft, fileNameMap.apply(getNeuronFile(ft)))
        );
    }

    public boolean hasNeuronComputeFile(ComputeFileType t) {
        return neuronComputeFiles.containsKey(t);
    }

    public String getNeuronComputeFile(ComputeFileType t) {
        return neuronComputeFiles.get(t);
    }

    public void setNeuronComputeFile(ComputeFileType t, String fn) {
        if (StringUtils.isNotBlank(fn)) {
            neuronComputeFiles.put(t, fn);
        } else {
            neuronComputeFiles.remove(t);
        }
    }

    public void putProcessedTags(ProcessingType processingType, Set<String> tags) {
        if (processingType != null && CollectionUtils.isNotEmpty(tags)) {
            processedTags.put(processingType, tags);
        }
    }

    public boolean hasAnyProcessedTag(ProcessingType processingType) {
        return CollectionUtils.isNotEmpty(processedTags.get(processingType));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        AbstractNeuronMetadata that = (AbstractNeuronMetadata) o;

        return new EqualsBuilder()
                .append(mipId, that.mipId)
                .append(getNeuronFile(FileType.ColorDepthMip), that.getNeuronFile(FileType.ColorDepthMip))
                .append(getNeuronFile(FileType.ColorDepthMipInput), that.getNeuronFile(FileType.ColorDepthMipInput))
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(mipId)
                .append(getNeuronFile(FileType.ColorDepthMip))
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
