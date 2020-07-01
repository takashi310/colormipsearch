package org.janelia.colormipsearch.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Results<T> {
    @JsonProperty
    public final T results;

    @JsonCreator
    public Results(@JsonProperty("results") T results) {
        this.results = results;
    }
}
