package org.janelia.colormipsearch.api.pppsearch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.api.FileType;

/**
 * These are the source PPP matches as they are imported from the original matches
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SourcePPPMatch {
    private String sourceEmName;
    private String neuronName; // em body ID
    private String neuronType;
    private String neuronInstance;
    private String neuronStatus;
    private String sourceLmName;
    private String lineName;
    private String sampleId;
    private String sampleName;
    private String slideCode;
    private String objective;
    private String mountingProtocol;
    private String alignmentSpace;
    private String gender;
    private Double coverageScore;
    private Double aggregateCoverage;
    private Boolean mirrored;
    private Double emPPPRank;
    private Map<FileType, String> files;
    private List<SkeletonMatch> skeletonMatches;

    public String getSourceEmName() {
        return sourceEmName;
    }

    public void setSourceEmName(String sourceEmName) {
        this.sourceEmName = sourceEmName;
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

    public String getSourceLmName() {
        return sourceLmName;
    }

    public void setSourceLmName(String sourceLmName) {
        this.sourceLmName = sourceLmName;
    }

    public String getLineName() {
        return lineName;
    }

    public void setLineName(String lineName) {
        this.lineName = lineName;
    }

    public String getSampleId() {
        return sampleId;
    }

    public void setSampleId(String sampleId) {
        this.sampleId = sampleId;
    }

    public String getSampleName() {
        return sampleName;
    }

    public void setSampleName(String sampleName) {
        this.sampleName = sampleName;
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

    public String getMountingProtocol() {
        return mountingProtocol;
    }

    public void setMountingProtocol(String mountingProtocol) {
        this.mountingProtocol = mountingProtocol;
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

    public boolean addSourceImageFile(String imageName) {
        FileType imageFileType = FileType.findFileType(imageName);
        if (imageFileType == null) {
            return false;
        } else {
            if (files == null) {
                files = new HashMap<>();
            }
            files.put(imageFileType, imageName);
            return true;
        }
    }

    public Map<FileType, String> getFiles() {
        return files;
    }

    public void setFiles(Map<FileType, String> files) {
        this.files = files;
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
                .append("fullEmName", sourceEmName)
                .append("fullLmName", sourceLmName)
                .append("coverageScore", coverageScore)
                .append("aggregateCoverage", aggregateCoverage)
                .toString();
    }
}
