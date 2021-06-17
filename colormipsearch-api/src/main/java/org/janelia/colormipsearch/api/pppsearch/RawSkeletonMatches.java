package org.janelia.colormipsearch.api.pppsearch;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class RawSkeletonMatches {
    @JsonProperty("all_skel_ids")
    private String allSkeletonIds;
    @JsonProperty("all_nblast_scores")
    private String allNBlastScores;
    @JsonProperty("all_coverages")
    private String allCoveragesScores;
    @JsonProperty("all_colors")
    private String allColors;
    @JsonProperty("cov_score")
    private Double coverageScore;
    @JsonProperty("aggregate_coverage")
    private Double aggregateCoverage;
    private Boolean mirrored;
    private Double rank;

    public String getAllSkeletonIds() {
        return allSkeletonIds;
    }

    public void setAllSkeletonIds(String allSkeletonIds) {
        this.allSkeletonIds = allSkeletonIds;
    }

    public String getAllNBlastScores() {
        return allNBlastScores;
    }

    public void setAllNBlastScores(String allNBlastScores) {
        this.allNBlastScores = allNBlastScores;
    }

    public String getAllCoveragesScores() {
        return allCoveragesScores;
    }

    public void setAllCoveragesScores(String allCoveragesScores) {
        this.allCoveragesScores = allCoveragesScores;
    }

    public String getAllColors() {
        return allColors;
    }

    public void setAllColors(String allColors) {
        this.allColors = allColors;
    }

    public Double getCoverageScore() {
        return coverageScore;
    }

    public void setCoverageScore(Double coverageScore) {
        this.coverageScore = coverageScore;
    }

    public Double getAggregateCoverage() {
        return aggregateCoverage;
    }

    public void setAggregateCoverage(Double aggregateCoverage) {
        this.aggregateCoverage = aggregateCoverage;
    }

    public Boolean getMirrored() {
        return mirrored;
    }

    public void setMirrored(Boolean mirrored) {
        this.mirrored = mirrored;
    }

    public Double getRank() {
        return rank;
    }

    public void setRank(Double rank) {
        this.rank = rank;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("allSkeletonIds", allSkeletonIds)
                .append("allNBlastScores", allNBlastScores)
                .append("allCoveragesScores", allCoveragesScores)
                .append("allColors", allColors)
                .append("coverageScore", coverageScore)
                .append("aggregateCoverage", aggregateCoverage)
                .append("mirrored", mirrored)
                .toString();
    }
}
