package org.janelia.colormipsearch.model;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
public abstract class AbstractMatch<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> {

    private M maskImage;
    private T matchedImage;
    private boolean mirrored; // if true the matchedImage was mirrored
    private Map<MatchComputeFileType, FileData> matchComputeFiles = new HashMap<>();
    private Map<FileType, FileData> matchFiles = new HashMap<>(); // match specific files

    public M getMaskImage() {
        return maskImage;
    }

    public void setMaskImage(M maskImage) {
        this.maskImage = maskImage;
    }

    public void resetMaskImage() {
        this.maskImage = null;
    }

    @JsonProperty("image")
    public T getMatchedImage() {
        return matchedImage;
    }

    public void setMatchedImage(T matchedImage) {
        this.matchedImage = matchedImage;
    }

    public void resetMatchedImage() {
        this.matchedImage = null;
    }

    public boolean isMirrored() {
        return mirrored;
    }

    public void setMirrored(boolean mirrored) {
        this.mirrored = mirrored;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<MatchComputeFileType, FileData> getMatchComputeFiles() {
        return matchComputeFiles;
    }

    public void setMatchComputeFiles(Map<MatchComputeFileType, FileData> matchComputeFiles) {
        this.matchComputeFiles = matchComputeFiles;
    }

    public FileData getMatchComputeFileData(MatchComputeFileType t) {
        return matchComputeFiles.get(t);
    }

    public void setMatchComputeFileData(MatchComputeFileType t, FileData fd) {
        if (fd != null) {
            matchComputeFiles.put(t, fd);
        } else {
            matchComputeFiles.remove(t);
        }
    }

    public void resetMatchComputeFiles() {
        matchComputeFiles.clear();
    }

    @JsonProperty("files")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<FileType, FileData> getMatchFiles() {
        return matchFiles;
    }

    public void setMatchFiles(Map<FileType, FileData> matchFiles) {
        this.matchFiles = matchFiles;
    }

    public FileData getMatchFileData(FileType t) {
        return matchFiles.get(t);
    }

    public void setMatchFileData(FileType t, FileData fd) {
        if (fd != null) {
            matchFiles.put(t, fd);
        } else {
            matchFiles.remove(t);
        }
    }

    public void resetMatchFiles() {
        matchFiles.clear();
    }

    /**
     * This method only copies data that can be safely assigned to the destination fields;
     * that is why it doess not copy the mask and the target images since the type for
     * those may not coincide with the ones from the source
     *
     * @param that
     * @param <M1> destination mask type
     * @param <T1> destination target type
     * @param <R1> destination result type
     */
    protected <M1 extends AbstractNeuronMetadata,
               T1 extends AbstractNeuronMetadata,
               R1 extends AbstractMatch<M1, T1>> void safeFieldsCopyFrom(R1 that) {
        this.mirrored = that.isMirrored();
        this.matchFiles.clear();
        this.matchFiles.putAll(that.getMatchFiles());
        this.matchComputeFiles.clear();
        this.matchComputeFiles.putAll(that.getMatchComputeFiles());
    }

    public abstract <M2 extends AbstractNeuronMetadata,
                     T2 extends AbstractNeuronMetadata> AbstractMatch<M2, T2> duplicate(MatchCopier<M, T, AbstractMatch<M, T>, M2, T2, AbstractMatch<M2, T2>> copier);

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        AbstractMatch<?, ?> that = (AbstractMatch<?, ?>) o;

        return new EqualsBuilder().append(mirrored, that.mirrored).append(maskImage, that.maskImage).append(matchedImage, that.matchedImage).append(matchComputeFiles, that.matchComputeFiles).append(matchFiles, that.matchFiles).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(maskImage).append(matchedImage).append(mirrored).append(matchComputeFiles).append(matchFiles).toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("maskImage", maskImage != null ? maskImage.getId() : "<null>")
                .append("matchedImage", matchedImage != null ? matchedImage.getId() : "<null>")
                .append("mirrored", mirrored)
                .append("matchComputeFiles", matchComputeFiles)
                .append("matchFiles", matchFiles)
                .toString();
    }
}
