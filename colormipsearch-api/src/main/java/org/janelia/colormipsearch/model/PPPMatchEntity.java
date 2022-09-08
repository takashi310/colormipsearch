package org.janelia.colormipsearch.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections4.MapUtils;
import org.janelia.colormipsearch.dto.AbstractNeuronMetadata;
import org.janelia.colormipsearch.dto.PPPMatchedTarget;
import org.janelia.colormipsearch.model.annotations.PersistenceInfo;

@PersistenceInfo(storeName ="pppMatches")
public class PPPMatchEntity<M extends AbstractNeuronEntity, T extends AbstractNeuronEntity> extends AbstractMatchEntity<M, T> {

    private static final Pattern EM_REG_EX_PATTERN = Pattern.compile("([0-9]+)-([^-]*)-(.*)", Pattern.CASE_INSENSITIVE);
    private static final Pattern LM_REG_EX_PATTERN = Pattern.compile("(.+)_REG_UNISEX_(.+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern OBJECTIVE_PATTERN = Pattern.compile("\\d+x", Pattern.CASE_INSENSITIVE);
    private static final String DEFAULT_OBJECTIVE = "40x";
    private static final String UNKNOWN_ALIGNMENT_SPACE = "UNKNOWN-AS";

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
        updateLMSampleInfo(m);
        generateMatchFiles(m);
        m.setMirrored(isMirrored());
        m.setRank(getRank());
        m.setScore((int)Math.abs(coverageScore));
        return m;
    }

    public String extractLMSampleName() {
        Matcher matcher = LM_REG_EX_PATTERN.matcher(getSourceLmName());
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return getSourceLmName();
        }
    }

    private String extractEMBodyID() {
        Matcher matcher = EM_REG_EX_PATTERN.matcher(getSourceEmName());
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return getSourceEmName();
        }
    }

    private void generateMatchFiles(PPPMatchedTarget<AbstractNeuronMetadata> m) {
        if (hasSourceImageFiles()) {
            sourceImageFiles.keySet().stream()
                    .flatMap(k -> k.getFileTypes().stream())
                    .forEach(ft -> m.setMatchFile(
                            ft,
                            buildImageRelativePath(
                                    getAlignmentSpace(),
                                    extractEMBodyID(),
                                    m.getSourceObjective(),
                                    ft.getDisplayPPPSuffix()
                            )
                    ));
        }
    }

    private String getAlignmentSpace() {
        if (getMaskImage() != null && getMaskImage().hasAlignmentSpace()) {
            return getMaskImage().getAlignmentSpace();
        } else if (getMatchedImage() != null && getMatchedImage().hasAlignmentSpace()) {
            return getMatchedImage().getAlignmentSpace();
        } else {
            return UNKNOWN_ALIGNMENT_SPACE;
        }
    }

    private String buildImageRelativePath(String alignmentSpace,
                                          String emNeuronName,
                                          String objective,
                                          String suffix) {
        return emNeuronName.substring(0, 2) + '/' +
                emNeuronName + '/' +
                emNeuronName + '-' +
                "{lmLine}" + "-" + // placeholder for LM line
                "{lmSlideCode}" + "-" + // placeholder for LM slide code
                objective + "-" +
                alignmentSpace + '-' +
                suffix;
    }

    private void updateLMSampleInfo(PPPMatchedTarget<AbstractNeuronMetadata> m) {
        m.setSourceLmName(getSourceLmName());
        m.setSourceObjective(DEFAULT_OBJECTIVE);
        m.setSourceLmLibrary(getSourceLmLibrary());
        Matcher matcher = LM_REG_EX_PATTERN.matcher(getSourceLmName());
        if (matcher.find()) {
            m.setSourceLmName(matcher.group(1));
            String objectiveCandidate = matcher.group(2);
            if (OBJECTIVE_PATTERN.matcher(objectiveCandidate).find()) {
                m.setSourceObjective(objectiveCandidate);
            }
        }
    }

}
