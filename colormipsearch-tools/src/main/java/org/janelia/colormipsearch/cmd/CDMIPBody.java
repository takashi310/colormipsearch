package org.janelia.colormipsearch.cmd;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * This is the representation of a JACS Sample.
 */
class CDMIPBody {
    @JsonProperty("_id")
    String id;
    @JsonProperty("name")
    String name;
    @JsonProperty
    Map<String, String> files;

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("name", name)
                .toString();
    }
}
