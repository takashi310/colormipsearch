package org.janelia.colormipsearch.cmd_v2;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.colormipsearch.model.Results;

public class LightColorMIPSearchResults extends Results<List<LightColorMIPSearchMatch>> {

    private final String sourceId;

    @JsonCreator
    public static LightColorMIPSearchResults createCDSMatches(@JsonProperty("maskId") String maskId,
                                                              @JsonProperty("results") List<LightColorMIPSearchMatch> results) {
        if (StringUtils.isNotBlank(maskId)) {
            results.forEach(csr -> {
                csr.setSourceId(maskId);
            });
        }
        return new LightColorMIPSearchResults(maskId, results);
    }

    private LightColorMIPSearchResults(String sourceId, List<LightColorMIPSearchMatch> results) {
        super(results);
        this.sourceId = sourceId;
    }

    boolean isEmpty() {
        return CollectionUtils.isEmpty(results);
    }

    /**
     * This is field will not written for every match in a result file because it only adds a lot of noise.
     *
     * @return
     */
    public String getSourceId() {
        return sourceId;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("sourceId", sourceId)
                .toString();
    }

}
