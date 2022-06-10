package org.janelia.colormipsearch.api_v2.cdsearch;

import java.io.Serializable;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.janelia.colormipsearch.api_v2.cdmips.MIPImage;

/**
 * Creates a color depth search for a given mask.
 */
public class ColorMIPSearch implements Serializable {

    private final ColorDepthSearchAlgorithmProvider<ColorMIPMatchScore> cdsAlgorithmProvider;
    private final Integer defaultQueryThreshold;
    private final Double pctPositivePixels;

    public ColorMIPSearch(Double pctPositivePixels,
                          Integer defaultQueryThreshold,
                          ColorDepthSearchAlgorithmProvider<ColorMIPMatchScore> cdsAlgorithmProvider) {
        this.pctPositivePixels = pctPositivePixels;
        this.defaultQueryThreshold = defaultQueryThreshold;
        this.cdsAlgorithmProvider = cdsAlgorithmProvider;
    }

    public Map<String, Object> getCDSParameters() {
        Map<String, Object> cdsParams = new LinkedHashMap<>(cdsAlgorithmProvider.getDefaultCDSParams().asMap());
        cdsParams.put("pctPositivePixels", pctPositivePixels != null ? pctPositivePixels.toString() : null);
        cdsParams.put("defaultMaskThreshold", defaultQueryThreshold != null ? defaultQueryThreshold.toString() : null);
        return cdsParams;
    }

    public ColorDepthSearchAlgorithm<ColorMIPMatchScore> createQueryColorDepthSearchWithDefaultThreshold(MIPImage queryMIPImage) {
        try {
            return cdsAlgorithmProvider.createColorDepthQuerySearchAlgorithmWithDefaultParams(queryMIPImage.getImageArray(), defaultQueryThreshold == null ? 0 : defaultQueryThreshold, 0);
        } catch(Exception e) {
            throw new IllegalStateException("Error creating a color depth search for " + queryMIPImage.toString(), e);
        }
    }

    public ColorDepthSearchAlgorithm<ColorMIPMatchScore> createQueryColorDepthSearch(MIPImage queryMIPImage, int queryThreshold, int borderSize) {
        return cdsAlgorithmProvider.createColorDepthQuerySearchAlgorithmWithDefaultParams(queryMIPImage.getImageArray(), queryThreshold, borderSize);
    }


    public boolean isMatch(ColorMIPMatchScore colorMIPMatchScore) {
        double pixMatchRatioThreshold = pctPositivePixels != null ? pctPositivePixels / 100 : 0.;
        return colorMIPMatchScore.getMatchingPixNumToMaskRatio() > pixMatchRatioThreshold;
    }

    public Comparator<ColorMIPSearchResult> getColorMIPSearchResultComparator() {
        return Comparator.comparingInt(ColorMIPSearchResult::getMatchingPixels).reversed();
    }

}
