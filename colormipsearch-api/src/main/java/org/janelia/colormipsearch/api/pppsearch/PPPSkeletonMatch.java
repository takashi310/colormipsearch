package org.janelia.colormipsearch.api.pppsearch;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonInclude;

import org.apache.commons.lang3.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PPPSkeletonMatch {

    static PPPSkeletonMatch fromSourceSkeletonMatch(SourceSkeletonMatch sourceSkeletonMatch) {
        PPPSkeletonMatch pppSkeletonMatch = new PPPSkeletonMatch();
        pppSkeletonMatch.id = sourceSkeletonMatch.getId();
        pppSkeletonMatch.nblastScore = sourceSkeletonMatch.getNblastScore();
        pppSkeletonMatch.coverage = sourceSkeletonMatch.getCoverage();
        return pppSkeletonMatch;
    }

    private String id;
    private Double nblastScore;
    private Double coverage;

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

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("nblastScore", nblastScore)
                .append("coverage", coverage)
                .toString();
    }
}
