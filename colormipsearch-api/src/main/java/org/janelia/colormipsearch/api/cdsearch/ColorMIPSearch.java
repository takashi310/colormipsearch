package org.janelia.colormipsearch.api.cdsearch;

import java.io.Serializable;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.janelia.colormipsearch.api.cdmips.MIPImage;

/**
 * Creates a color depth search for a given mask.
 */
public class ColorMIPSearch implements Serializable {

    private final ColorDepthSearchAlgorithmProvider<ColorMIPMatchScore> cdsAlgorithmProvider;
    private final Double pctPositivePixels;

    public ColorMIPSearch(Double pctPositivePixels,
                          ColorDepthSearchAlgorithmProvider<ColorMIPMatchScore> cdsAlgorithmProvider) {
        this.pctPositivePixels = pctPositivePixels;
        this.cdsAlgorithmProvider = cdsAlgorithmProvider;
    }

    public Map<String, Object> getCDSParameters() {
        Map<String, Object> cdsParams = new LinkedHashMap<>(cdsAlgorithmProvider.getDefaultCDSParams().asMap());
        cdsParams.put("pctPositivePixels", pctPositivePixels != null ? pctPositivePixels.toString() : null);
        return cdsParams;
    }

    public ColorDepthSearchAlgorithm<ColorMIPMatchScore> createQueryColorDepthSearch(MIPImage queryMIPImage, Integer queryThresholdParam) {
        ColorDepthSearchParams querySpecificParams = new ColorDepthSearchParams().setParam("maskThreshold", queryThresholdParam);
        return cdsAlgorithmProvider.createColorDepthQuerySearchAlgorithm(queryMIPImage.getImageArray(), querySpecificParams);
    }

    public boolean isMatch(ColorMIPMatchScore colorMIPMatchScore) {
        double pixMatchRatioThreshold = pctPositivePixels != null ? pctPositivePixels / 100 : 0.;
        return colorMIPMatchScore.getMatchingPixNumToMaskRatio() > pixMatchRatioThreshold;
    }

    public Comparator<ColorMIPSearchResult> getColorMIPSearchResultComparator() {
        return Comparator.comparingInt(ColorMIPSearchResult::getMatchingPixels).reversed();
    }

}
