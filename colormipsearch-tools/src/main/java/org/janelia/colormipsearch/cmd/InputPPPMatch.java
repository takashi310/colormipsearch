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
}
