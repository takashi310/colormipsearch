package org.janelia.colormipsearch.dto;

import javax.validation.GroupSequence;

/**
 * The default group validation for PPP target images is different because PPP targets will not have a MIP ID.
 */
@GroupSequence({LMPPPNeuronMetadata.class, MissingSomeRequiredAttrs.class})
public class LMPPPNeuronMetadata extends LMNeuronMetadata {

    public LMPPPNeuronMetadata() {
    }

    public LMPPPNeuronMetadata(LMNeuronMetadata source) {
        this();
        copyFrom(source);
    }
}
