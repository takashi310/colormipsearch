package org.janelia.colormipsearch;

import com.fasterxml.jackson.annotation.JsonProperty;

class ColorDepthMetadata extends MetadataAttrs {
    @JsonProperty
    String internalName;
    @JsonProperty
    String sampleRef;
}
