package org.janelia.colormipsearch.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MIPMatchesCount {
    @JsonProperty("_id")
    private String mipId;
    private Long cdMatchesCountAsMask;
    private Long cdMatchesCountAsTarget;
    private Long pppMatchesCount;

    public String getMipId() {
        return mipId;
    }

    public void setMipId(String mipId) {
        this.mipId = mipId;
    }

    public Long getCdMatchesCountAsMask() {
        return cdMatchesCountAsMask;
    }

    public void setCdMatchesCountAsMask(Long cdMatchesCountAsMask) {
        this.cdMatchesCountAsMask = cdMatchesCountAsMask;
    }

    public Long getCdMatchesCountAsTarget() {
        return cdMatchesCountAsTarget;
    }

    public void setCdMatchesCountAsTarget(Long cdMatchesCountAsTarget) {
        this.cdMatchesCountAsTarget = cdMatchesCountAsTarget;
    }

    public Long getPppMatchesCount() {
        return pppMatchesCount;
    }

    public void setPppMatchesCount(Long pppMatchesCount) {
        this.pppMatchesCount = pppMatchesCount;
    }
}
