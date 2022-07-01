package org.janelia.colormipsearch.model;

import org.apache.commons.lang3.StringUtils;

public enum MatchComputeFileType {
    MatchedColorDepthImage,
    MatchedGradientImage,
    MatchedZGapImage;

    public static MatchComputeFileType fromName(String name) {
        for (MatchComputeFileType vt : values()) {
            if (StringUtils.equalsIgnoreCase(vt.name(), name)) {
                return vt;
            }
        }
        return null;
    }

}
