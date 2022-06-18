package org.janelia.colormipsearch.model;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class LMNeuronMetadata extends AbstractNeuronMetadata {

    private String sampleRef;
    private String slideCode;
    private String objective;
    private String anatomicalArea;
    private String mountingProtocol;
    private String driver;
    private Integer channel; // 1-based channel number

    @Override
    public String getNeuronId() {
        return getSlideCode();
    }

    public String getSampleRef() {
        return sampleRef;
    }

    public void setSampleRef(String sampleRef) {
        this.sampleRef = sampleRef;
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

    public String getAnatomicalArea() {
        return anatomicalArea;
    }

    public void setAnatomicalArea(String anatomicalArea) {
        this.anatomicalArea = anatomicalArea;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
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
    @SuppressWarnings("unchecked")
    public <N extends AbstractNeuronMetadata> N duplicate() {
        LMNeuronMetadata n = new LMNeuronMetadata();
        n.copyFrom(this);
        n.sampleRef = this.sampleRef;
        n.slideCode = this.slideCode;
        n.objective = this.objective;
        n.anatomicalArea = this.anatomicalArea;
        n.mountingProtocol = this.mountingProtocol;
        n.channel = this.channel;
        return (N) n;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("slideCode", slideCode)
                .append("objective", objective)
                .append("channel", channel)
                .toString();
    }
}
