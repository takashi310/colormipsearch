package org.janelia.colormipsearch.api.pppsearch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonIncludeProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.api.FileType;

/**
 * These are the source PPP matches as they are imported from the original matches.
 * This object contains all fields currently read from the original result file.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "id", "publishedName", "libraryName",
        "pppRank", "pppScore",
        "sampleName", "slideCode", "objective",
        "gender", "alignmentSpace", "mountingProtocol",
        "coverageScore", "aggregateCoverage", "mirrored",
        "files", "sourceImageFiles", "skeletonMatches"
})
public class SourcePPPMatch {
    @JsonIgnore
    private String sourceEmName;
    @JsonIgnore
    private String sourceEmDataset;
    @JsonIgnore
    private String neuronName; // EM body ID
    @JsonIgnore
    private String neuronType;
    @JsonIgnore
    private String neuronInstance;
    @JsonIgnore
    private String neuronStatus;
    @JsonIgnore
    private String sourceLmName;
    @JsonProperty("libraryName")
    private String sourceLmDataset;
    @JsonProperty("publishedName")
    private String lineName; // LM line
    @JsonProperty("id")
    private String sampleId; // LM sample ID
    private String sampleName; // LM sample name
    private String slideCode;
    private String objective;
    private String mountingProtocol;
    private String alignmentSpace;
    private String gender;
    private Double coverageScore;
    private Double aggregateCoverage;
    private Boolean mirrored;
    @JsonProperty("pppRank")
    private Double emPPPRank;
    private Map<FileType, String> sourceImageFiles;
    private List<SourceSkeletonMatch> skeletonMatches;

    public String getSourceEmName() {
        return sourceEmName;
    }

    public void setSourceEmName(String sourceEmName) {
        this.sourceEmName = sourceEmName;
    }

    public String getSourceEmDataset() {
        return sourceEmDataset;
    }

    public void setSourceEmDataset(String sourceEmDataset) {
        this.sourceEmDataset = sourceEmDataset;
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

    public String getSourceLmDataset() {
        return sourceLmDataset;
    }

    public void setSourceLmDataset(String sourceLmDataset) {
        this.sourceLmDataset = sourceLmDataset;
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

    public Boolean getMirrored() {
        return mirrored;
    }

    public void setMirrored(Boolean mirrored) {
        this.mirrored = mirrored;
    }

    @JsonProperty
    public int getPPPScore() {
        return coverageScore == null ? 0 : (int)Math.abs(coverageScore);
    }

    @JsonIgnore
    void setPPPScore() {
        // pppScore is Read Only
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

    @JsonProperty
    public Map<FileType, String> getFiles() {
        return this.sourceImageFiles == null
                ? null
                : sourceImageFiles.keySet().stream().collect(Collectors.toMap(e -> e, this::getTargetImageRelativePath));
    }

    @JsonIgnore
    void setFiles(Map<FileType, String> sourceImageFiles) {
        // do nothing here
    }

    private String getTargetImageRelativePath(FileType ft) {
        // e.g. "12/1200351200/1200351200-VT064583-20170630_64_C2-40x-JRC2018_Unisex_20x_HR-masked_inst.png"
        StringBuilder fileNameBuilder = new StringBuilder()
                .append(neuronName.substring(0,2))
                .append('/')
                .append(neuronName)
                .append('/')
                .append(neuronName)
                .append('-')
                .append(lineName)
                .append('-')
                .append(slideCode)
                .append('-')
                .append(objective)
                .append('-')
                .append(alignmentSpace)
                .append('-')
                .append(ft.getSuffix())
                ;
        return fileNameBuilder.toString();
    }

    public void addSourceImageFile(String imageName) {
        FileType imageFileType = FileType.findFileType(imageName);
        if (imageFileType != null) {
            if (sourceImageFiles == null) {
                sourceImageFiles = new HashMap<>();
            }
            sourceImageFiles.put(imageFileType, imageName);
        }
    }

    public Map<FileType, String> getSourceImageFiles() {
        return sourceImageFiles;
    }

    public void setSourceImageFiles(Map<FileType, String> sourceImageFiles) {
        this.sourceImageFiles = sourceImageFiles;
    }

    public boolean hasSkeletonMatches() {
        return CollectionUtils.isNotEmpty(skeletonMatches);
    }

    public List<SourceSkeletonMatch> getSkeletonMatches() {
        return skeletonMatches;
    }

    public void setSkeletonMatches(List<SourceSkeletonMatch> skeletonMatches) {
        this.skeletonMatches = skeletonMatches;
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
