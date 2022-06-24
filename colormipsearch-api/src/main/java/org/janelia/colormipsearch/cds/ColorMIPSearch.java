package org.janelia.colormipsearch.cds;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import org.janelia.colormipsearch.mips.NeuronMIP;

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

    public ColorDepthSearchAlgorithm<ColorMIPMatchScore> createQueryColorDepthSearchWithDefaultThreshold(NeuronMIP<?> queryMIPImage) {
        try {
            return cdsAlgorithmProvider.createColorDepthQuerySearchAlgorithmWithDefaultParams(queryMIPImage.getImageArray(), defaultQueryThreshold == null ? 0 : defaultQueryThreshold, 0);
        } catch(Exception e) {
            throw new IllegalStateException("Error creating a color depth search for " + queryMIPImage.toString(), e);
        }
    }

    public ColorDepthSearchAlgorithm<ColorMIPMatchScore> createQueryColorDepthSearch(NeuronMIP<?> queryMIPImage, int queryThreshold, int borderSize) {
        return cdsAlgorithmProvider.createColorDepthQuerySearchAlgorithmWithDefaultParams(queryMIPImage.getImageArray(), queryThreshold, borderSize);
    }


    public boolean isMatch(ColorMIPMatchScore colorMIPMatchScore) {
        double pixMatchRatioThreshold = pctPositivePixels != null ? pctPositivePixels / 100 : 0.;
        return colorMIPMatchScore.getMatchingPixNum() > 0 && colorMIPMatchScore.getMatchingPixNumToMaskRatio() > pixMatchRatioThreshold;
    }

}
