package org.janelia.colormipsearch.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SingleMatchResult<I extends AbstractNeuronImage, R extends AbstractMatch<? extends AbstractNeuronImage>> {
    private I input;
    private R match;

    @JsonProperty("inputImage")
    public I getInput() {
        return input;
    }

    public void setInput(I input) {
        this.input = input;
    }

    @JsonProperty("matchedImage")
    public R getMatch() {
        return match;
    }

    public void setMatch(R match) {
        this.match = match;
    }
}
