package org.janelia.colormipsearch.api.pppsearch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.api.FileType;

/**
 * These are the source PPP matches as they are imported from the original matches.
 * This object contains all fields currently read from the original result file.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(
        fieldVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE
)
public class AbstractPPPMatch {

    public static <S extends AbstractPPPMatch, D extends AbstractPPPMatch> D copyFrom(S from, D to) {
        to.setSourceEmName(from.getSourceEmName());
        to.setSourceEmDataset(from.getSourceEmDataset());
        to.setNeuronId(from.getNeuronId());
        to.setNeuronName(from.getNeuronName());
        to.setNeuronType(from.getNeuronType());
        to.setNeuronInstance(from.getNeuronInstance());
        to.setNeuronStatus(from.getNeuronStatus());
        to.setSourceLmName(from.getSourceLmName());
        to.setSourceLmDataset(from.getSourceLmDataset());
        to.setLineName(from.getLineName());
        to.setSampleId(from.getSampleId());
        to.setSampleName(from.getSampleName());
        to.setSlideCode(from.getSlideCode());
        to.setAnatomicalArea(from.getAnatomicalArea());
        to.setObjective(from.getObjective());
        to.setMountingProtocol(from.getMountingProtocol());
        to.setAlignmentSpace(from.getAlignmentSpace());
        to.setGender(from.getGender());
        to.setCoverageScore(from.getCoverageScore());
        to.setAggregateCoverage(from.getAggregateCoverage());
        to.setMirrored(from.getMirrored());
        to.setEmPPPRank(from.getEmPPPRank());
        to.setSourceImageFiles(from.getSourceImageFiles());
        to.setSkeletonMatches(from.getSkeletonMatches());
        return to;
    }

    public static class Update<M extends AbstractPPPMatch> {
        private final M pppMatch;

        public Update(M pppMatch) {
            this.pppMatch = pppMatch;
        }

        public <F> Update<M> applyUpdate(BiConsumer<M, F> update, F toUpdate) {
            update.accept(pppMatch, toUpdate);
            return this;
        }

        public M get() {
            return pppMatch;
        }
    }

    private String sourceEmName;
    private String sourceEmDataset;
    private String neuronId; // JACS EM body ID
    private String neuronName; // EM body ID
    private String neuronType;
    private String neuronInstance;
    private String neuronStatus;
    private String sourceLmName;
    private String sourceLmDataset;
    private String lineName; // LM line
    private String sampleId; // LM sample ID
    private String sampleName; // LM sample name
    private String slideCode;
    private String anatomicalArea;
    private String objective;
    private String mountingProtocol;
    private String alignmentSpace;
    private String gender;
    private Double coverageScore;
    private Double aggregateCoverage;
    private Boolean mirrored;
    private Double emPPPRank;
    private Map<PPPScreenshotType, String> sourceImageFiles;
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

    public String getNeuronId() {
        return neuronId;
    }

    public void setNeuronId(String neuronId) {
        this.neuronId = neuronId;
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

    @JsonProperty
    public String getAnatomicalArea() {
        return anatomicalArea;
    }

    public void setAnatomicalArea(String anatomicalArea) {
        this.anatomicalArea = anatomicalArea;
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

    @JsonProperty
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

    @JsonProperty
    public Double getCoverageScore() {
        return coverageScore;
    }

    public void setCoverageScore(Double coverageScore) {
        this.coverageScore = coverageScore;
    }

    @JsonProperty
    public Double getAggregateCoverage() {
        return aggregateCoverage;
    }

    public void setAggregateCoverage(Double aggregateCoverage) {
        this.aggregateCoverage = aggregateCoverage;
    }

    @JsonProperty
    public Boolean getMirrored() {
        return mirrored;
    }

    public void setMirrored(Boolean mirrored) {
        this.mirrored = mirrored;
    }

    @JsonProperty("pppScore")
    public int getPPPScore() {
        return coverageScore == null ? 0 : (int)Math.abs(coverageScore);
    }

    @JsonIgnore
    void setPPPScore(int pppScore) {
        coverageScore = pppScore == 0 ? null : new Double(-pppScore);
    }

    boolean hasEmPPPRank() {
        return emPPPRank != null;
    }

    @JsonProperty("pppRank")
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
                : sourceImageFiles.keySet().stream().collect(Collectors.toMap(
                e -> e.getFileType(),
                e -> getTargetImageRelativePath(e.getFileType())));
    }

    @JsonIgnore
    void setFiles(Map<FileType, String> files) {
        // do nothing here
    }

    private String getTargetImageRelativePath(FileType ft) {
        // e.g. "12/1200351200/1200351200-VT064583-20170630_64_C2-40x-JRC2018_Unisex_20x_HR-masked_inst.png"
        return neuronName.substring(0, 2) + '/' +
                neuronName + '/' +
                neuronName + '-' +
                lineName + '-' +
                slideCode + '-' +
                objective + '-' +
                alignmentSpace + '-' +
                ft.getDisplaySuffix();
    }

    public void addSourceImageFile(String imageName) {
        PPPScreenshotType imageFileType = PPPScreenshotType.findScreenshotType(imageName);
        if (imageFileType != null) {
            if (sourceImageFiles == null) {
                sourceImageFiles = new HashMap<>();
            }
            sourceImageFiles.put(imageFileType, imageName);
        }
    }

    public boolean hasSourceImageFiles() {
        return MapUtils.isNotEmpty(sourceImageFiles);
    }

    @JsonProperty
    public Map<PPPScreenshotType, String> getSourceImageFiles() {
        return sourceImageFiles;
    }

    public void setSourceImageFiles(Map<PPPScreenshotType, String> sourceImageFiles) {
        this.sourceImageFiles = sourceImageFiles;
    }

    public boolean hasSkeletonMatches() {
        return CollectionUtils.isNotEmpty(skeletonMatches);
    }

    @JsonProperty
    public List<SourceSkeletonMatch> getSkeletonMatches() {
        return skeletonMatches;
    }

    public void setSkeletonMatches(List<SourceSkeletonMatch> skeletonMatches) {
        this.skeletonMatches = skeletonMatches;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("sourceEmName", sourceEmName)
                .append("sourceLmName", sourceLmName)
                .append("coverageScore", coverageScore)
                .append("aggregateCoverage", aggregateCoverage)
                .toString();
    }
}
