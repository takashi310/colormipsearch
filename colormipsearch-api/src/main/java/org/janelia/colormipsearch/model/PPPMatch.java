package org.janelia.colormipsearch.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;

public class PPPMatch<M extends AbstractNeuronMetadata, I extends AbstractNeuronMetadata> extends AbstractMatch<M, I> {
    private String sourceEmName;
    private String sourceLmName;
    private Double coverageScore;
    private Double aggregateCoverage;
    private Double rank;
    private Map<PPPScreenshotType, String> sourceImageFiles;
    private List<PPPSkeletonMatch> skeletonMatches;

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

    public void updateMatchFiles() {
        if (sourceImageFiles != null) {
            sourceImageFiles.keySet()
                    .forEach(k -> setMatchFileData(k.getFileType(), getTargetImageRelativePath(k.getFileType())));
        }
    }

    private FileData getTargetImageRelativePath(FileType ft) {
        // e.g. "12/1200351200/1200351200-VT064583-20170630_64_C2-40x-JRC2018_Unisex_20x_HR-masked_inst.png"
        String maskPublishedName = getMaskImage().getPublishedName();
        return FileData.fromString(
                maskPublishedName.substring(0, 2) + '/' +
                maskPublishedName + '/' +
                maskPublishedName + '-' +
                getMatchedImage().buildNeuronSourceName() + "-" +
                getMaskImage().getAlignmentSpace() + '-' +
                ft.getDisplayPPPSuffix()
        );
    }

    public List<PPPSkeletonMatch> getSkeletonMatches() {
        return skeletonMatches;
    }

    public void setSkeletonMatches(List<PPPSkeletonMatch> skeletonMatches) {
        this.skeletonMatches = skeletonMatches;
    }
}
