package org.janelia.colormipsearch.cmd_v2;

import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated
public class EMNeuron {
    @JsonProperty("_id")
    String id;
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
