package org.janelia.colormipsearch.cmd;

import javax.annotation.Nonnull;

import org.janelia.colormipsearch.cmd.jacsdata.CDMIPBody;
import org.janelia.colormipsearch.cmd.jacsdata.CDMIPSample;
import org.janelia.colormipsearch.cmd.jacsdata.ColorDepthMIP;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.EMNeuronEntity;
import org.janelia.colormipsearch.model.LMNeuronEntity;
import org.janelia.colormipsearch.model.PPPMatchEntity;

class InputPPPMatch {

    private final PPPMatchEntity<EMNeuronEntity, LMNeuronEntity> pppMatch;
    private String emNeuronName;
    private String emNeuronType;
    private CDMIPBody emBody;
    private String lmSampleName;
    private String lmObjective;
    private CDMIPSample lmSample;

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

    String getEmNeuronType() {
        return emNeuronType;
    }

    void setEmNeuronType(String emNeuronType) {
        this.emNeuronType = emNeuronType;
    }

    String getLmSampleName() {
        return lmSampleName;
    }

    void setLmSampleName(String lmSampleName) {
        this.lmSampleName = lmSampleName;
    }

    String getLmObjective() {
        return lmObjective;
    }

    void setLmObjective(String lmObjective) {
        this.lmObjective = lmObjective;
    }

    CDMIPBody getEmBody() {
        return emBody;
    }

    void setEmBody(CDMIPBody emBody) {
        this.emBody = emBody;
    }

    String getEmId() {
        return emBody != null ? "EMBody#" + emBody.id : null;
    }

    CDMIPSample getLmSample() {
        return lmSample;
    }

    void setLmSample(CDMIPSample lmSample) {
        this.lmSample = lmSample;
    }

    String getLmId() {
        return lmSample != null ? "Sample#" + lmSample.id : null;
    }

    String getLmLineName() {
        return lmSample != null ? lmSample.publishingName : null;
    }

    String getLmNeuronName() {
        return lmSample != null
                ? lmSample.publishingName + "-" + lmSample.slideCode
                : lmSampleName;
    }
}
