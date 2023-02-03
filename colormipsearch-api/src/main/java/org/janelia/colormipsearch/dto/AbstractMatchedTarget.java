package org.janelia.colormipsearch.dto;

import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.model.FileType;

/**
 * This is the representation of a matched target. It only contains information about the target image not about the mask.
 *
 * @param <T> target neuron type
 */
public abstract class AbstractMatchedTarget<T extends AbstractNeuronMetadata> {

    // Keep the mask internal ID a little longer because we'll need it to retrieve the corresponding image URLs
    private Number matchInternalId;
    private Number maskImageInternalId;
    private T targetImage;
    private boolean mirrored;
    private final Map<FileType, String> matchFiles = new HashMap<>(); // match specific files

    @JsonIgnore
    public Number getMatchInternalId() {
        return matchInternalId;
    }

    public void setMatchInternalId(Number matchInternalId) {
        this.matchInternalId = matchInternalId;
    }

    @JsonProperty("type")
    public abstract String getTypeDiscriminator();

    @JsonIgnore
    void setTypeDiscriminator(String discriminator) {
        // do nothing - this is a read only property
    }

    /**
     * The mask internal ID will not be externalized. We only need it to retrieve the corresponding image URLs.
     * @return
     */
    @JsonIgnore
    public Number getMaskImageInternalId() {
        return maskImageInternalId;
    }

    public void setMaskImageInternalId(Number maskImageInternalId) {
        this.maskImageInternalId = maskImageInternalId;
    }

    @NotNull
    @JsonProperty("image")
    public T getTargetImage() {
        return targetImage;
    }

    public void setTargetImage(T targetImage) {
        this.targetImage = targetImage;
    }

    public boolean isMirrored() {
        return mirrored;
    }

    public void setMirrored(boolean mirrored) {
        this.mirrored = mirrored;
    }

    @NotNull
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

    public boolean hasMatchFiles() {
        return !matchFiles.isEmpty();
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
