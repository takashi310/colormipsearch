package org.janelia.colormipsearch.cmd;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EMNeuron {
    @JsonProperty
    String name;
    @JsonProperty
    String type;
    @JsonProperty
    String instance;
    @JsonProperty
    String status;
}
