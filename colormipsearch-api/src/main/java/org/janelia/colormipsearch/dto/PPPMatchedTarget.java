package org.janelia.colormipsearch.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;

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
    private Double coverageScore;
    private Double aggregateCoverage;

    public Double getRank() {
        return rank;
    }

    public void setRank(Double rank) {
        this.rank = rank;
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
