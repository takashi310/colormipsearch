package org.janelia.colormipsearch.cmd.jacsdata;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * This is the representation of a JACS EM Body.
 */
public class CDMIPBody {

    public static Map<String, CDMIPBody> indexByRef(List<CDMIPBody> emBodies) {
        return emBodies.stream().collect(Collectors.toMap(n -> "EMBody#" + n.id, n -> n));
    }

    @JsonProperty("_id")
    public String id;
    @JsonProperty("name")
    public String name;
    @JsonProperty
    public String neuronType;
    @JsonProperty
    public String neuronInstance;
    @JsonProperty
    public String status;
    @JsonProperty("dataSetIdentifier")
    public String datasetIdentifier;
    @JsonProperty
    public Map<String, String> files;

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("name", name)
                .toString();
    }
}
