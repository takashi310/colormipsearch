package org.janelia.colormipsearch.dto;

/**
 * Metadata about a PPP matched target.
 *
 * @param <T>
 */
public class PPPMatchedTarget<T extends AbstractNeuronMetadata> extends AbstractMatchedTarget<T> {
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
}
