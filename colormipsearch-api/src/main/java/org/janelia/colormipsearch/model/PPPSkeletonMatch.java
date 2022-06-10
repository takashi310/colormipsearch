package org.janelia.colormipsearch.model;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import org.apache.commons.lang3.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PPPSkeletonMatch {
    private String id;
    private Double nblastScore;
    private Double coverage;
    @JsonIgnore
    private short[] colors;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Nonnull
    public Double getNblastScore() {
        return nblastScore;
    }

    public void setNblastScore(Double nblastScore) {
        this.nblastScore = nblastScore;
    }

    public Double getCoverage() {
        return coverage;
    }

    public void setCoverage(Double coverage) {
        this.coverage = coverage;
    }

    public short[] getColors() {
        return colors;
    }

    public void setColors(short[] colors) {
        this.colors = colors;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("nblastScore", nblastScore)
                .append("coverage", coverage)
                .toString();
    }
}
