package org.janelia.colormipsearch.model;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class EMNeuronMetadata extends AbstractNeuronMetadata {

    private String bodyRef;
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
    public String buildNeuronSourceName() {
        return getPublishedName() + "-" + getNeuronType() + "-" + getState();
    }

    @Override
    public EMNeuronMetadata duplicate() {
        EMNeuronMetadata n = new EMNeuronMetadata();
        n.copyFrom(this);
        n.bodyRef = this.bodyRef;
        n.neuronType = this.neuronType;
        n.neuronInstance = this.neuronInstance;
        n.state = this.state;
        return n;
    }

    @Override
    public void cleanupForRelease() {
        super.cleanupForRelease();
        bodyRef = null;
    }

    @Override
    public List<Pair<String, ?>> updatableFields() {
        return Stream.concat(
                super.updatableFields().stream(),
                Stream.of(
                        ImmutablePair.of("bodyRef", getBodyRef()),
                        ImmutablePair.of("neuronType", getNeuronType()),
                        ImmutablePair.of("neuronInstance", getNeuronInstance()),
                        ImmutablePair.of("state", getState())
                )
        ).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("neuronType", neuronType)
                .append("neuronInstance", neuronInstance)
                .toString();
    }
}
