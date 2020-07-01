package org.janelia.colormipsearch.api.cdmips;

import java.nio.file.Paths;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.commons.lang3.StringUtils;

public class MIPMetadata extends AbstractMetadata {
    private String relatedImageRefId;

    public String getRelatedImageRefId() {
        return relatedImageRefId;
    }

    public void setRelatedImageRefId(String relatedImageRefId) {
        this.relatedImageRefId = relatedImageRefId;
    }

    @JsonIgnore
    public String getCdmName() {
        if (StringUtils.isNotBlank(getCdmPath())) {
            return Paths.get(getCdmPath()).getFileName().toString();
        } else {
            return null;
        }
    }
}
