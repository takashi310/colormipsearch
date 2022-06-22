package org.janelia.colormipsearch.model;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class EMNeuronMetadata extends AbstractNeuronMetadata {

    private String bodyRef;
    private String emName;
    private String neuronType;
    private String neuronInstance;
    private String state;

    @Override
    public String getNeuronId() {
        return getPublishedName();
    }

    public String getBodyRef() {
        return bodyRef;
    }

    public void setBodyRef(String bodyRef) {
        this.bodyRef = bodyRef;
    }

    public String getEmName() {
        return emName;
    }

    public void setEmName(String emName) {
        this.emName = emName;
    }

    public String getNeuronType() {
        return neuronType;
    }

    public void setNeuronType(String neuronType) {
        this.neuronType = neuronType;
    }

    public String getNeuronInstance() {
        return neuronInstance;
    }

    public void setNeuronInstance(String neuronInstance) {
        this.neuronInstance = neuronInstance;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <N extends AbstractNeuronMetadata> N duplicate() {
        EMNeuronMetadata n = new EMNeuronMetadata();
        n.copyFrom(this);
        n.emName = this.emName;
        n.neuronType = this.neuronType;
        n.neuronInstance = this.neuronInstance;
        n.state = this.state;
        return (N) n;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("neuronType", neuronType)
                .append("neuronInstance", neuronInstance)
                .toString();
    }
}
