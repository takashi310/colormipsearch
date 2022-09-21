package org.janelia.colormipsearch.dto;

import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

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
}
