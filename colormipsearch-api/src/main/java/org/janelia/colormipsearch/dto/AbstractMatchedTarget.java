package org.janelia.colormipsearch.dto;

import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.FileData;
import org.janelia.colormipsearch.model.FileType;
import org.janelia.colormipsearch.model.JsonRequired;

/**
 * This is the representation of a matched target. It only contains information about the target image not about the mask.
 *
 * @param <T> target neuron type
 */
public abstract class AbstractMatchedTarget<T extends AbstractNeuronMetadata> {

    private T targetImage;
    private boolean mirrored;
    private final Map<FileType, String> matchFiles = new HashMap<>(); // match specific files

    @NotNull
    @JsonRequired
    @JsonProperty("image")
    public T getTargetImage() {
        return targetImage;
    }

    public void setTargetImage(T targetImage) {
        this.targetImage = targetImage;
    }

    @JsonRequired
    public boolean isMirrored() {
        return mirrored;
    }

    public void setMirrored(boolean mirrored) {
        this.mirrored = mirrored;
    }

    @NotNull
    @JsonRequired
    @JsonProperty("files")
    public Map<FileType, String> getMatchFiles() {
        return matchFiles;
    }

    public void setMatchFiles(Map<FileType, String> matchFiles) {
        if (matchFiles != null) {
            this.matchFiles.putAll(matchFiles);
        }
    }

    public String getMatchFile(FileType t) {
        return matchFiles.get(t);
    }

    public void setMatchFile(FileType t, String fn) {
        if (StringUtils.isNotBlank(fn)) {
            matchFiles.put(t, fn);
        } else {
            matchFiles.remove(t);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        AbstractMatchedTarget<?> that = (AbstractMatchedTarget<?>) o;

        return new EqualsBuilder()
                .append(mirrored, that.mirrored)
                .append(targetImage, that.targetImage)
                .append(matchFiles, that.matchFiles)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(targetImage)
                .append(mirrored)
                .append(matchFiles)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("targetImage", targetImage != null ? targetImage.getMipId() : "<null>")
                .append("mirrored", mirrored)
                .toString();
    }
}
