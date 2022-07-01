package org.janelia.colormipsearch.model;

import org.apache.commons.lang3.StringUtils;

public enum MatchComputeFileType {
    MaskColorDepthImage,
    MaskGradientImage,
    MaskZGapImage;

    public static MatchComputeFileType fromName(String name) {
        for (MatchComputeFileType vt : values()) {
            if (StringUtils.equalsIgnoreCase(vt.name(), name)) {
                return vt;
            }
        }
        return null;
    }

}
