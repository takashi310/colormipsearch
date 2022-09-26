package org.janelia.colormipsearch.dto;

import javax.validation.GroupSequence;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The default group validation for PPP target images is different because PPP targets will not have a MIP ID.
 */
@GroupSequence({LMPPPNeuronMetadata.class, MissingSomeRequiredAttrs.class})
public class LMPPPNeuronMetadata extends LMNeuronMetadata {

    private String sampleId;

    public LMPPPNeuronMetadata() {
    }

    public LMPPPNeuronMetadata(LMNeuronMetadata source) {
        this();
        copyFrom(source);
    }

    @JsonIgnore
    @Override
    public String getMipId() {
        return super.getMipId();
    }

    @JsonProperty("id")
    public String getSampleId() {
        return sampleId;
    }

    public void setSampleId(String sampleId) {
        this.sampleId = sampleId;
    }
}
