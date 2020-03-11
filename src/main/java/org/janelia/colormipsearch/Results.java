package org.janelia.colormipsearch;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

class Results<T> {
    @JsonProperty
    T results;

    @JsonCreator
    public Results(@JsonProperty("results") T results) {
        this.results = results;
    }
}
