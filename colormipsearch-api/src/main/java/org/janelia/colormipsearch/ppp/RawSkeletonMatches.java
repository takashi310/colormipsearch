package org.janelia.colormipsearch.ppp;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.builder.ToStringBuilder;

class RawSkeletonMatches {
    @JsonProperty("skel_ids")
    private String bestSkeletonIds;
    @JsonProperty("nblast_scores")
    private String bestNBlastScores;
    @JsonProperty("coverages")
    private String bestCoveragesScores;
    @JsonProperty("colors")
    private String bestColors;
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

    String getBestSkeletonIds() {
        return bestSkeletonIds;
    }

    void setBestSkeletonIds(String bestSkeletonIds) {
        this.bestSkeletonIds = bestSkeletonIds;
    }

    String getBestNBlastScores() {
        return bestNBlastScores;
    }

    void setBestNBlastScores(String bestNBlastScores) {
        this.bestNBlastScores = bestNBlastScores;
    }

    String getBestCoveragesScores() {
        return bestCoveragesScores;
    }

    void setBestCoveragesScores(String bestCoveragesScores) {
        this.bestCoveragesScores = bestCoveragesScores;
    }

    String getBestColors() {
        return bestColors;
    }

    void setBestColors(String bestColors) {
        this.bestColors = bestColors;
    }

    String getAllSkeletonIds() {
        return allSkeletonIds;
    }

    void setAllSkeletonIds(String allSkeletonIds) {
        this.allSkeletonIds = allSkeletonIds;
    }

    String getAllNBlastScores() {
        return allNBlastScores;
    }

    void setAllNBlastScores(String allNBlastScores) {
        this.allNBlastScores = allNBlastScores;
    }

    String getAllCoveragesScores() {
        return allCoveragesScores;
    }

    void setAllCoveragesScores(String allCoveragesScores) {
        this.allCoveragesScores = allCoveragesScores;
    }

    String getAllColors() {
        return allColors;
    }

    void setAllColors(String allColors) {
        this.allColors = allColors;
    }

    Double getCoverageScore() {
        return coverageScore;
    }

    void setCoverageScore(Double coverageScore) {
        this.coverageScore = coverageScore;
    }

    Double getAggregateCoverage() {
        return aggregateCoverage;
    }

    void setAggregateCoverage(Double aggregateCoverage) {
        this.aggregateCoverage = aggregateCoverage;
    }

    boolean isMirrored() {
        return mirrored != null && mirrored;
    }

    void setMirrored(Boolean mirrored) {
        this.mirrored = mirrored;
    }

    Double getRank() {
        return rank;
    }

    void setRank(Double rank) {
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
