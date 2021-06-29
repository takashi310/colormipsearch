package org.janelia.colormipsearch.api.pppsearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.api.FileType;

/**
 * These are the source PPP matches for
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PPPMatch {

    public static PPPMatch fromSourcePPPMatch(SourcePPPMatch sourcePPPMatch) {
        PPPMatch pppMatch = new PPPMatch();
        pppMatch.neuronName = sourcePPPMatch.getNeuronName(); // EM body ID
        pppMatch.neuronType = sourcePPPMatch.getNeuronType();
        pppMatch.neuronInstance = sourcePPPMatch.getNeuronInstance();
        pppMatch.lineName = sourcePPPMatch.getLineName(); // LM published line name
        pppMatch.sampleId = sourcePPPMatch.getSampleId(); // LM sample ID
        pppMatch.slideCode = sourcePPPMatch.getSlideCode();
        pppMatch.objective = sourcePPPMatch.getObjective();
        pppMatch.mountingProtocol = sourcePPPMatch.getMountingProtocol();
        pppMatch.alignmentSpace = sourcePPPMatch.getAlignmentSpace();
        pppMatch.gender = sourcePPPMatch.getGender();
        pppMatch.coverageScore = sourcePPPMatch.getCoverageScore();
        pppMatch.aggregateCoverage = sourcePPPMatch.getAggregateCoverage();
        pppMatch.mirrored = sourcePPPMatch.getMirrored();
        pppMatch.emPPPRank = sourcePPPMatch.getEmPPPRank();
        sourcePPPMatch.handleFiles((ft, fn) -> {
            // e.g. "1200351200-VT064583-20170630_64_C2-40x-JRC2018_Unisex_20x_HR-masked_inst.png"
            StringBuilder fileNameBuilder = new StringBuilder()
                    .append(pppMatch.neuronName)
                    .append('-')
                    .append(pppMatch.lineName)
                    .append('-')
                    .append(pppMatch.slideCode)
                    .append('-')
                    .append(pppMatch.objective)
                    .append('-')
                    .append(pppMatch.alignmentSpace)
                    .append('-')
                    .append(ft.getSuffix())
                    ;
            pppMatch.addFile(ft, fileNameBuilder.toString());
        });
        if (sourcePPPMatch.hasSkeletonMatches()) {
            sourcePPPMatch.getSkeletonMatches().forEach(sm -> pppMatch.addSkeletonMatch(PPPSkeletonMatch.fromSourceSkeletonMatch(sm)));
        }
        return pppMatch;
    }

    private String neuronName; // EM body ID
    private String neuronDataset;
    private String neuronType;
    private String neuronInstance;
    private String neuronStatus;
    private String lineName; // LM published line name
    private String lineDataset;
    private String sampleId; // LM sample ID
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
    private List<PPPSkeletonMatch> skeletonMatches;

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

    public Double getEmPPPRank() {
        return emPPPRank;
    }

    public void setEmPPPRank(Double emPPPRank) {
        this.emPPPRank = emPPPRank;
    }

    public Map<FileType, String> getFiles() {
        return files;
    }

    public void setFiles(Map<FileType, String> files) {
        this.files = files;
    }

    public void addFile(FileType ft, String fileName) {
        if (files == null) {
            files = new HashMap<>();
        }
        files.put(ft, fileName);
    }

    public List<PPPSkeletonMatch> getSkeletonMatches() {
        return skeletonMatches;
    }

    public void setSkeletonMatches(List<PPPSkeletonMatch> skeletonMatches) {
        this.skeletonMatches = skeletonMatches;
    }

    public boolean hasSkeletonMatches() {
        return CollectionUtils.isNotEmpty(skeletonMatches);
    }

    private void addSkeletonMatch(PPPSkeletonMatch skeletonMatch) {
        if (skeletonMatches == null) {
            skeletonMatches = new ArrayList<>();
        }
        skeletonMatches.add(skeletonMatch);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("neuronName", neuronName)
                .append("lineName", lineName)
                .append("emPPPRank", emPPPRank)
                .toString();
    }
}
