package org.janelia.colormipsearch.model;

import org.apache.commons.lang3.StringUtils;

public enum ComputeFileType {
    SourceColorDepthImage,
    InputColorDepthImage,
    GradientImage,
    ZGapImage,
    MatchedColorDepthImage,
    MatchedGradientImage,
    MatchedZGapImage,
    SWCBody;

    public static ComputeFileType fromName(String name) {
        for (ComputeFileType vt : values()) {
            if (StringUtils.equalsIgnoreCase(vt.name(), name)) {
                return vt;
            }
        }
        return null;
    }

}
