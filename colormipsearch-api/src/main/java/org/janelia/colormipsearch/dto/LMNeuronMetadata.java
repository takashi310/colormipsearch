package org.janelia.colormipsearch.dto;

import javax.validation.constraints.NotBlank;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.model.JsonRequired;

public class LMNeuronMetadata extends AbstractNeuronMetadata {
    private String slideCode;
    private String objective;
    private String mountingProtocol;
    private Integer channel; // 1-based channel number

    @NotBlank
    @JsonRequired
    public String getSlideCode() {
        return slideCode;
    }

    public void setSlideCode(String slideCode) {
        this.slideCode = slideCode;
    }

    @NotBlank
    @JsonRequired
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

    public Integer getChannel() {
        return channel;
    }

    public void setChannel(Integer channel) {
        this.channel = channel;
    }

    @Override
    protected <N extends AbstractNeuronMetadata> void copyFrom(N that) {
        super.copyFrom(that);
        LMNeuronMetadata lmNeuronMetadata = (LMNeuronMetadata) that;
        this.slideCode = lmNeuronMetadata.slideCode;
        this.objective = lmNeuronMetadata.objective;
        this.mountingProtocol = lmNeuronMetadata.mountingProtocol;
        this.channel = lmNeuronMetadata.channel;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("slideCode", getSlideCode())
                .toString();
    }
}
