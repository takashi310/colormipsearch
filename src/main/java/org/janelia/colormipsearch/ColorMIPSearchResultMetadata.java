package org.janelia.colormipsearch;

import com.fasterxml.jackson.annotation.JsonProperty;

class ColorMIPSearchResultMetadata extends MetadataAttrs {
    @JsonProperty
    String matchedId;
}
