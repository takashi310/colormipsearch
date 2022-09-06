package org.janelia.colormipsearch.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.dto.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.annotations.PersistenceInfo;

@PersistenceInfo(storeName ="neuronMetadata")
public abstract class AbstractNeuronEntity extends AbstractBaseEntity {
    public static final String NO_CONSENSUS = "No Consensus";

    // MIP ID that comes from the Workstation (JACS).
    // This does not uniquely identify the metadata because there may be multiple images,
    // such as segmentation or FL images, that will be actually used for matching this MIP ID.
    // Do not mix this with the entityId which is the primary key of this entity
    private String mipId;
    // MIP alignment space
    private String alignmentSpace;
    // MIP library name
    private String libraryName;
    // neuron published name - this field is required during the gradient score process for selecting the top ranked matches
    private String publishedName;
    // Source Ref ID - either the LM Sample Reference ID or EM Body Reference ID.
    // This will be used to identify the matched neurons for PPP since the color depth MIPs are not
    // part of the PPP match process at all.
    private String sourceRefId;
    // computeFileData holds local files used either for precompute or upload
    private final Map<ComputeFileType, FileData> computeFiles = new HashMap<>();

    public String getMipId() {
        return mipId;
    }

    public void setMipId(String mipId) {
        this.mipId = mipId;
    }

    public boolean hasMipID() {
        return StringUtils.isNotBlank(mipId);
    }

    @JsonIgnore
    public abstract String getNeuronId();

    public String getAlignmentSpace() {
        return alignmentSpace;
    }

    public void setAlignmentSpace(String alignmentSpace) {
        this.alignmentSpace = alignmentSpace;
    }

    public boolean hasAlignmentSpace() {
        return StringUtils.isNotBlank(alignmentSpace);
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

    public String getSourceRefId() {
        return sourceRefId;
    }

    public void setSourceRefId(String sourceRefId) {
        this.sourceRefId = sourceRefId;
    }

    @JsonProperty
    public Map<ComputeFileType, FileData> getComputeFiles() {
        return computeFiles;
    }

    void setComputeFiles(Map<ComputeFileType, FileData> computeFiles) {
        if (computeFiles != null) {
            this.computeFiles.putAll(computeFiles);
        }
    }

    public FileData getComputeFileData(ComputeFileType t) {
        return computeFiles.get(t);
    }

    public void setComputeFileData(ComputeFileType t, FileData fd) {
        if (fd != null) {
            computeFiles.put(t, fd);
        } else {
            computeFiles.remove(t);
        }
    }

    public void resetComputeFileData(Set<ComputeFileType> ts) {
        ts.forEach(computeFiles::remove);
    }

    public String getComputeFileName(ComputeFileType t) {
        FileData f = computeFiles.get(t);
        return f != null ? f.getName() : null;
    }

    public boolean hasComputeFile(ComputeFileType t) {
        return computeFiles.containsKey(t);
    }

    public abstract AbstractNeuronEntity duplicate();

    public abstract AbstractNeuronMetadata metadata();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        AbstractNeuronEntity that = (AbstractNeuronEntity) o;

        return new EqualsBuilder()
                .appendSuper(super.equals(o))
                .append(mipId, that.mipId)
                .append(getComputeFileData(ComputeFileType.SourceColorDepthImage), that.getComputeFileData(ComputeFileType.SourceColorDepthImage))
                .append(getComputeFileData(ComputeFileType.InputColorDepthImage), that.getComputeFileData(ComputeFileType.InputColorDepthImage))
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(mipId)
                .append(getComputeFileData(ComputeFileType.SourceColorDepthImage))
                .append(getComputeFileData(ComputeFileType.InputColorDepthImage))
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("mipId", mipId)
                .append("libraryName", libraryName)
                .append("inputImage", getComputeFileName(ComputeFileType.InputColorDepthImage))
                .toString();
    }

    protected <N extends AbstractNeuronEntity> void copyFrom(N that) {
        this.mipId = that.getMipId();
        this.alignmentSpace = that.getAlignmentSpace();
        this.libraryName = that.getLibraryName();
        this.publishedName = that.getPublishedName();
        this.sourceRefId = that.getSourceRefId();
        this.computeFiles.clear();
        this.computeFiles.putAll(that.getComputeFiles());
        this.addAllTags(that.getTags());
    }

}
