package org.janelia.colormipsearch.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class ListMatchResults<I extends AbstractNeuronImage, R extends AbstractMatch<? extends AbstractNeuronImage>> {
    private I input;
    private List<R> matches;

    @JsonProperty("inputImage")
    public I getInput() {
        return input;
    }

    public void setInput(I input) {
        this.input = input;
    }

    @JsonProperty("results")
    public List<R> getMatches() {
        return matches;
    }

    public void setMatches(List<R> matches) {
        this.matches = matches;
    }
}
