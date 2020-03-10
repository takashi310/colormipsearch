package org.janelia.colormipsearch;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang.StringUtils;

class ColorMIPSearchResultMetadata extends MetadataAttrs {
    @JsonProperty
    String matchedId;

    @JsonIgnore
    public int getMatchingSlices() {
        String matchingSlices = getAttr("Matched slices");
        return StringUtils.isBlank(matchingSlices) ? 0 : Integer.parseInt(matchingSlices);
    }

    public void setMatchingSlices(int matchingSlices) {
        if (matchingSlices > 0) {
            addAttr("Matched slices", String.valueOf(matchingSlices));
        }
    }

    @JsonIgnore
    public double getMatchingSlicesPct() {
        String matchingSlicesPct = getAttr("Matched slices");
        return StringUtils.isBlank(matchingSlicesPct) ? 0. : Double.parseDouble(matchingSlicesPct);
    }

    public void setMatchingSlicesPct(double matchingSlicesPct) {
        if (matchingSlicesPct > 0.) {
            addAttr("Matched slices", String.valueOf(matchingSlicesPct));
        }
    }

    @JsonIgnore
    public long getGradientAreaGap() {
        String gradientAreaGap = getAttr("Gradient Area Gap");
        return StringUtils.isBlank(gradientAreaGap) ? -1 : Long.parseLong(gradientAreaGap);
    }

    public void setGradientAreaGap(long gradientAreaGap) {
        if (gradientAreaGap >= 0L) {
            addAttr("Gradient Area Gap", String.valueOf(gradientAreaGap));
        }
    }
}
