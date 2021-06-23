package org.janelia.colormipsearch.api.pppsearch;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PPPMatch {
    private String fullEmName;
    private String neuronName; // bodyId
    private String neuronType;
    private String neuronInstance;
    private String neuronStatus;
    private String fullLmName;
    private String lmSampleName;
    private String lineName;
    private String slideCode;
    private String objective;
    private String alignmentSpace;
    private String gender;
    private Double coverageScore;
    private Double aggregateCoverage;
    private Boolean mirrored;
    private Double emPPPRank;
    private List<PPPImageVariant> imageVariants;
    private List<SkeletonMatch> skeletonMatches;

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

    public String getNeuronStatus() {
        return neuronStatus;
    }

    public void setNeuronStatus(String neuronStatus) {
        this.neuronStatus = neuronStatus;
    }

    public String getFullLmName() {
        return fullLmName;
    }

    public void setFullLmName(String fullLmName) {
        this.fullLmName = fullLmName;
    }

    public String getLmSampleName() {
        return lmSampleName;
    }

    public void setLmSampleName(String lmSampleName) {
        this.lmSampleName = lmSampleName;
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

    public String getObjective() {
        return objective;
    }

    public void setObjective(String objective) {
        this.objective = objective;
    }

    public String getAlignmentSpace() {
        return alignmentSpace;
    }

    public void setAlignmentSpace(String alignmentSpace) {
        this.alignmentSpace = alignmentSpace;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
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

    public boolean addImageVariant(String imageName) {
        PPPImageVariant.VariantType variantType = PPPImageVariant.VariantType.findVariantType(imageName);
        if (variantType == null) {
            return false;
        } else {
            if (imageVariants == null) {
                imageVariants = new ArrayList<>();
            }
            imageVariants.add(new PPPImageVariant(variantType, imageName));
            return true;
        }
    }

    public List<PPPImageVariant> getImageVariants() {
        return imageVariants;
    }

    public void setImageVariants(List<PPPImageVariant> imageVariants) {
        this.imageVariants = imageVariants;
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

    boolean hasEmPPPRank() {
        return emPPPRank != null;
    }

    public Double getEmPPPRank() {
        return emPPPRank;
    }

    public void setEmPPPRank(Double emPPPRank) {
        this.emPPPRank = emPPPRank;
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
