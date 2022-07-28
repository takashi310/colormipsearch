package org.janelia.colormipsearch.results;

import org.janelia.colormipsearch.model.Gender;
import org.janelia.colormipsearch.model.LMNeuronEntity;

class TestLMNeuronEntity extends LMNeuronEntity {
    private String sampleRef;
    private String sampleName;
    private String objective;
    private Gender gender;
    private String anatomicalArea;
    private String mountingProtocol;
    private Integer channel;

    public String getSampleRef() {
        return sampleRef;
    }

    public void setSampleRef(String sampleRef) {
        this.sampleRef = sampleRef;
    }

    public String getSampleName() {
        return sampleName;
    }

    public void setSampleName(String sampleName) {
        this.sampleName = sampleName;
    }

    public String getObjective() {
        return objective;
    }

    public void setObjective(String objective) {
        this.objective = objective;
    }

    public Gender getGender() {
        return gender;
    }

    public void setGender(Gender gender) {
        this.gender = gender;
    }

    public String getAnatomicalArea() {
        return anatomicalArea;
    }

    public void setAnatomicalArea(String anatomicalArea) {
        this.anatomicalArea = anatomicalArea;
    }

    public String getMountingProtocol() {
        return mountingProtocol;
    }

    public void setMountingProtocol(String mountingProtocol) {
        this.mountingProtocol = mountingProtocol;
    }

    public Integer getChannel() {
        return channel;
    }

    public void setChannel(Integer channel) {
        this.channel = channel;
    }

    @Override
    public TestLMNeuronEntity duplicate() {
        TestLMNeuronEntity n = new TestLMNeuronEntity();
        n.copyFrom(this);
        return n;
    }
}
