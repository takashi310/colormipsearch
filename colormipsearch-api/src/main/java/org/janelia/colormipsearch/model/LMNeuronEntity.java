package org.janelia.colormipsearch.model;

import java.util.HashMap;
import java.util.Map;

import org.janelia.colormipsearch.dto.LMNeuronMetadata;

public class LMNeuronEntity extends AbstractNeuronEntity {

    // LM slide code is required for selecting the top ranked matches during gradient scoring
    private String slideCode;
    // anatomicalArea, gender and objective required for uploading the imagery
    private String anatomicalArea;
    private Gender gender;
    private String objective;

    @Override
    public String getNeuronId() {
        return getSlideCode();
    }

    public String getSlideCode() {
        return slideCode;
    }

    public void setSlideCode(String slideCode) {
        this.slideCode = slideCode;
    }

    public String getAnatomicalArea() {
        return anatomicalArea;
    }

    public void setAnatomicalArea(String anatomicalArea) {
        this.anatomicalArea = anatomicalArea;
    }

    public Gender getGender() {
        return gender;
    }

    public void setGender(Gender gender) {
        this.gender = gender;
    }

    public String getObjective() {
        return objective;
    }

    public void setObjective(String objective) {
        this.objective = objective;
    }

    @Override
    public Map<String, Object> updateableFieldValues() {
        Map<String, Object> dict = new HashMap<>(super.updateableFieldValues());
        dict.put("slideCode", slideCode);
        dict.put("anatomicalArea", anatomicalArea);
        dict.put("gender", gender);
        dict.put("objective", objective);
        return dict;
    }

    @Override
    public LMNeuronEntity duplicate() {
        LMNeuronEntity n = new LMNeuronEntity();
        n.copyFrom(this);
        n.slideCode = this.getSlideCode();
        n.anatomicalArea = this.getAnatomicalArea();
        n.gender = this.getGender();
        n.objective = this.getObjective();
        return n;
    }

    @Override
    public LMNeuronMetadata metadata() {
        LMNeuronMetadata n = new LMNeuronMetadata();
        n.setInternalId(getEntityId());
        n.setAlignmentSpace(getAlignmentSpace());
        n.setMipId(getMipId());
        n.setLibraryName(getLibraryName());
        n.setPublishedName(getPublishedName());
        n.setSlideCode(slideCode);
        n.setAnatomicalArea(anatomicalArea);
        n.setGender(gender);
        n.setObjective(objective);
        getComputeFiles().forEach((ft, fd) -> n.setNeuronComputeFile(ft, fd.getFileName()));
        getProcessedTags().forEach(n::putProcessedTags);
        return n;
    }
}
