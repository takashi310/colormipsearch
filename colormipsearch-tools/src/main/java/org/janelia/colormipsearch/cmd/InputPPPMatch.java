package org.janelia.colormipsearch.cmd;

import javax.annotation.Nonnull;

import org.janelia.colormipsearch.cmd.jacsdata.CDMIPBody;
import org.janelia.colormipsearch.cmd.jacsdata.CDMIPSample;
import org.janelia.colormipsearch.cmd.jacsdata.ColorDepthMIP;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.EMNeuronEntity;
import org.janelia.colormipsearch.model.LMNeuronEntity;
import org.janelia.colormipsearch.model.PPPMatchEntity;

/**
 * This is type used only for holding temporary data needed during the PPP import process.
 * So far we only need the neuron body ID (emNeuronName) in order to retrieve and fill in
 * the proper mask image reference.
 */
class InputPPPMatch {

    private final PPPMatchEntity<EMNeuronEntity, LMNeuronEntity> pppMatch;
    private String emNeuronName;
    private String lmSampleName;
    private String lmPublishedName;
    private String lmSlideCode;
    private String lmObjective;

    InputPPPMatch(@Nonnull PPPMatchEntity<EMNeuronEntity, LMNeuronEntity> pppMatch) {
        this.pppMatch = pppMatch;
    }

    PPPMatchEntity<EMNeuronEntity, LMNeuronEntity> getPPPMatch() {
        return pppMatch;
    }

    String getEmNeuronName() {
        return emNeuronName;
    }

    void setEmNeuronName(String emNeuronName) {
        this.emNeuronName = emNeuronName;
    }

    public String getLmSampleName() {
        return lmSampleName;
    }

    public void setLmSampleName(String lmSampleName) {
        this.lmSampleName = lmSampleName;
    }

    public String getLmPublishedName() {
        return lmPublishedName;
    }

    public void setLmPublishedName(String lmPublishedName) {
        this.lmPublishedName = lmPublishedName;
    }

    public String getLmSlideCode() {
        return lmSlideCode;
    }

    public void setLmSlideCode(String lmSlideCode) {
        this.lmSlideCode = lmSlideCode;
    }

    public String getLmObjective() {
        return lmObjective;
    }

    public void setLmObjective(String lmObjective) {
        this.lmObjective = lmObjective;
    }
}
