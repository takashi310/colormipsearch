package org.janelia.colormipsearch.model;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class EMNeuronMetadata extends AbstractNeuronMetadata {

    private String neuronType;
    private String neuronInstance;

    @Override
    public String getNeuronId() {
        return getPublishedName();
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

    @Override
    @SuppressWarnings("unchecked")
    public <N extends AbstractNeuronMetadata> N duplicate() {
        EMNeuronMetadata n = new EMNeuronMetadata();
        n.copyFrom(this);
        n.neuronType = this.neuronType;
        n.neuronInstance = this.neuronInstance;
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
