package org.janelia.colormipsearch.cmd;

import com.fasterxml.jackson.annotation.JsonProperty;

class CDMIPSample {
    @JsonProperty("_id")
    String id;
    @JsonProperty
    String name;
    @JsonProperty
    String dataSet;
    @JsonProperty
    String gender;
    @JsonProperty
    String mountingProtocol;
    @JsonProperty
    String organism;
    @JsonProperty
    String genotype;
    @JsonProperty
    String flycoreId;
    @JsonProperty
    String line;
    @JsonProperty
    String slideCode;
    @JsonProperty
    String publishingName;
    @JsonProperty
    Boolean publishedToStaging;
    @JsonProperty
    String publishedExternally;
    @JsonProperty
    String crossBarcode;
    @JsonProperty
    String sampleRef;
    @JsonProperty
    String status;
}
