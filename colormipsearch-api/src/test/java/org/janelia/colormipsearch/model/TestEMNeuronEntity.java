package org.janelia.colormipsearch.model;

public class TestEMNeuronEntity extends EMNeuronEntity {
    private String neuronType;
    private String neuronInstance;
    private Gender gender;
    private String anatomicalArea;

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

    public Gender getGender() {
        return gender;
    }

    public void setGender(Gender gender) {
        this.gender = gender;
    }

    public String getAnatomicalArea() {
        return anatomicalArea;
    }

    public void setAnatomicalArea(String anatomicalArea) {
        this.anatomicalArea = anatomicalArea;
    }

    @Override
    public TestEMNeuronEntity duplicate() {
        TestEMNeuronEntity n = new TestEMNeuronEntity();
        n.copyFrom(this);
        return n;
    }
}
