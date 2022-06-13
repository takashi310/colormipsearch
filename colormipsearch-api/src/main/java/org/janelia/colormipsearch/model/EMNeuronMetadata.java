package org.janelia.colormipsearch.model;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class EMNeuronMetadata extends AbstractNeuronMetadata {

    private String neuronType;
    private String neuronInstance;

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
    public String toString() {
        return new ToStringBuilder(this)
                .append("neuronType", neuronType)
                .append("neuronInstance", neuronInstance)
                .toString();
    }
}
