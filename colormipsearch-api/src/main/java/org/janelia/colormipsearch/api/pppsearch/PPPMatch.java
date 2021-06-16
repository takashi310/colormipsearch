package org.janelia.colormipsearch.api.pppsearch;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class PPPMatch {
    private String fullEmName;
    private String neuronName; // bodyId
    private String neuronType;
    private String neuronInstance;
    private String fullLmName;
    private String lineName;
    private String slideCode;
    private String alignmentSpace;
    private String objective;
    private Double coverageScore;
    private Double aggregateCoverage;
    private List<SkeletonMatch> skeletonMatches;
    private Boolean mirrored;

    public String getFullEmName() {
        return fullEmName;
    }

    public void setFullEmName(String fullEmName) {
        this.fullEmName = fullEmName;
    }

    public String getNeuronName() {
        return neuronName;
    }

    public void setNeuronName(String neuronName) {
        this.neuronName = neuronName;
    }

    public String getNeuronType() {
        return neuronType;
    }

    public void setNeuronType(String neuronType) {
        this.neuronType = neuronType;
    }

    public String getNeuronInstance() {
        return neuronInstance;
    }

    public void setNeuronInstance(String neuronInstance) {
        this.neuronInstance = neuronInstance;
    }

    public String getFullLmName() {
        return fullLmName;
    }

    public void setFullLmName(String fullLmName) {
        this.fullLmName = fullLmName;
    }

    public String getLineName() {
        return lineName;
    }

    public void setLineName(String lineName) {
        this.lineName = lineName;
    }

    public String getSlideCode() {
        return slideCode;
    }

    public void setSlideCode(String slideCode) {
        this.slideCode = slideCode;
    }

    public String getAlignmentSpace() {
        return alignmentSpace;
    }

    public void setAlignmentSpace(String alignmentSpace) {
        this.alignmentSpace = alignmentSpace;
    }

    public String getObjective() {
        return objective;
    }

    public void setObjective(String objective) {
        this.objective = objective;
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

    public boolean hasSkeletonMatches() {
        return CollectionUtils.isNotEmpty(skeletonMatches);
    }

    public List<SkeletonMatch> getSkeletonMatches() {
        return skeletonMatches;
    }

    public void setSkeletonMatches(List<SkeletonMatch> skeletonMatches) {
        this.skeletonMatches = skeletonMatches;
    }

    public Boolean getMirrored() {
        return mirrored;
    }

    public void setMirrored(Boolean mirrored) {
        this.mirrored = mirrored;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("fullEmName", fullEmName)
                .append("fullLmName", fullLmName)
                .append("coverageScore", coverageScore)
                .append("aggregateCoverage", aggregateCoverage)
                .toString();
    }
}
