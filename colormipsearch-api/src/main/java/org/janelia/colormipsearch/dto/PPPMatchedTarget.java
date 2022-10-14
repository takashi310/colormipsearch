package org.janelia.colormipsearch.dto;

import java.util.HashSet;
import java.util.Set;

import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.janelia.colormipsearch.model.PPPScreenshotType;

/**
 * Metadata about a PPP matched target.
 *
 * @param <T>
 */
public class PPPMatchedTarget<T extends AbstractNeuronMetadata> extends AbstractMatchedTarget<T> {
    private String sourceLmName;
    private String sourceObjective;
    private String sourceLmLibrary;
    private Double rank;
    private int score;
    private Set<PPPScreenshotType> sourceImageFilesTypes = new HashSet<>();

    @Override
    public String getTypeDiscriminator() {
        return "PPPMatch";
    }

    @NotNull
    @JsonProperty("pppmRank")
    public Double getRank() {
        return rank;
    }

    public void setRank(Double rank) {
        this.rank = rank;
    }

    @JsonProperty("pppmScore")
    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    @JsonIgnore
    public String getSourceLmName() {
        return sourceLmName;
    }

    public void setSourceLmName(String sourceLmName) {
        this.sourceLmName = sourceLmName;
    }

    @JsonIgnore
    public String getSourceObjective() {
        return sourceObjective;
    }

    public void setSourceObjective(String sourceObjective) {
        this.sourceObjective = sourceObjective;
    }

    @JsonIgnore
    public String getSourceLmLibrary() {
        return sourceLmLibrary;
    }

    public void setSourceLmLibrary(String sourceLmLibrary) {
        this.sourceLmLibrary = sourceLmLibrary;
    }

    public boolean hasSourceImageFiles() {
        return !sourceImageFilesTypes.isEmpty();
    }

    @JsonIgnore
    public Set<PPPScreenshotType> getSourceImageFilesTypes() {
        return sourceImageFilesTypes;
    }

    public void addSourceImageFileTypes(Set<PPPScreenshotType> pppScreenshotTypes) {
        this.sourceImageFilesTypes.addAll(pppScreenshotTypes);
    }
}
