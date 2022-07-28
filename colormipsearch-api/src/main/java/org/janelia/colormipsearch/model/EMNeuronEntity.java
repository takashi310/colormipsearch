package org.janelia.colormipsearch.model;

public class EMNeuronEntity extends AbstractNeuronEntity {

    @Override
    public String getNeuronId() {
        return getPublishedName();
    }

    @Override
    public EMNeuronEntity duplicate() {
        EMNeuronEntity n = new EMNeuronEntity();
        n.copyFrom(this);
        return n;
    }
}
