package org.janelia.colormipsearch.cmd;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EMNeuron {
    @JsonProperty
    String name;
    @JsonProperty
    String neuronType;
    @JsonProperty
    String neuronInstance;
    @JsonProperty
    String status;
    @JsonProperty("dataSetIdentifier")
    String datasetIdentifier;
}
