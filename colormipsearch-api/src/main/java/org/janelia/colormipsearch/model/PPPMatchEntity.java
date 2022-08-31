package org.janelia.colormipsearch.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.janelia.colormipsearch.dto.AbstractNeuronMetadata;
import org.janelia.colormipsearch.dto.PPPMatchedTarget;
import org.janelia.colormipsearch.model.annotations.PersistenceInfo;

@PersistenceInfo(storeName ="pppMatches")
public class PPPMatchEntity<M extends AbstractNeuronEntity, T extends AbstractNeuronEntity> extends AbstractMatchEntity<M, T> {
    private String sourceEmName;
    private String sourceEmLibrary;
    private String sourceLmName;
    private String sourceLmLibrary;
    private Double coverageScore;
    private Double aggregateCoverage;
    private Double rank;
    private Map<PPPScreenshotType, String> sourceImageFiles;
    private List<PPPSkeletonMatch> skeletonMatches;

    public String getSourceEmLibrary() {
        return sourceEmLibrary;
    }

    public void setSourceEmLibrary(String sourceEmLibrary) {
        this.sourceEmLibrary = sourceEmLibrary;
    }

    public String getSourceEmName() {
        return sourceEmName;
    }

    public void setSourceEmName(String sourceEmName) {
        this.sourceEmName = sourceEmName;
    }

    public String getSourceLmName() {
        return sourceLmName;
    }

    public void setSourceLmName(String sourceLmName) {
        this.sourceLmName = sourceLmName;
    }

    public String getSourceLmLibrary() {
        return sourceLmLibrary;
    }

    public void setSourceLmLibrary(String sourceLmLibrary) {
        this.sourceLmLibrary = sourceLmLibrary;
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

    public Double getRank() {
        return rank;
    }

    public void setRank(Double rank) {
        this.rank = rank;
    }

    public Map<PPPScreenshotType, String> getSourceImageFiles() {
        return sourceImageFiles;
    }

    public void setSourceImageFiles(Map<PPPScreenshotType, String> sourceImageFiles) {
        this.sourceImageFiles = sourceImageFiles;
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

    public List<PPPSkeletonMatch> getSkeletonMatches() {
        return skeletonMatches;
    }

    public void setSkeletonMatches(List<PPPSkeletonMatch> skeletonMatches) {
        this.skeletonMatches = skeletonMatches;
    }

    @SuppressWarnings("unchecked")
    @Override
    public PPPMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity> duplicate(
            MatchCopier<AbstractMatchEntity<AbstractNeuronEntity, AbstractNeuronEntity>, AbstractMatchEntity<AbstractNeuronEntity, AbstractNeuronEntity>> copier) {
        PPPMatchEntity<AbstractNeuronEntity, AbstractNeuronEntity> clone = new PPPMatchEntity<>();
        // copy fields that are safe to copy
        clone.safeFieldsCopyFrom(this);
        // copy fields specific to this class
        clone.sourceEmName = this.sourceEmName;
        clone.sourceEmLibrary = this.sourceEmLibrary;
        clone.sourceLmName = this.sourceLmName;
        clone.sourceLmLibrary = this.sourceLmLibrary;
        clone.coverageScore = this.coverageScore;
        clone.aggregateCoverage = this.aggregateCoverage;
        clone.rank = this.rank;
        clone.sourceImageFiles = this.sourceImageFiles;
        clone.skeletonMatches = this.skeletonMatches;
        // apply the copier
        copier.copy((AbstractMatchEntity<AbstractNeuronEntity, AbstractNeuronEntity>) this, clone);
        return clone;
    }

    @Override
    public PPPMatchedTarget<? extends AbstractNeuronMetadata> metadata() {
        PPPMatchedTarget<AbstractNeuronMetadata> m = new PPPMatchedTarget<>();
        T matchedImage = getMatchedImage();
        if (matchedImage != null) {
            AbstractNeuronMetadata n = getMatchedImage().metadata();
            m.setTargetImage(n);
        }
        m.setSourceLmName(getSourceLmName());
        m.setSourceLmLibrary(getSourceLmLibrary());
        m.setMirrored(isMirrored());
        m.setRank(getRank());
        m.setCoverageScore(getCoverageScore());
        m.setAggregateCoverage(getAggregateCoverage());
        return m;
    }

}
