package org.janelia.colormipsearch.cds;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import org.janelia.colormipsearch.imageprocessing.ImageArray;

/**
 * Creates a color depth search for a given mask.
 */
public class ColorMIPSearch implements Serializable {

    private final ColorDepthSearchAlgorithmProvider<ColorDepthPixelMatchScore> cdsAlgorithmProvider;
    private final Integer defaultQueryThreshold;
    private final Double pctPositivePixels;

    public ColorMIPSearch(Double pctPositivePixels,
                          Integer defaultQueryThreshold,
                          ColorDepthSearchAlgorithmProvider<ColorDepthPixelMatchScore> cdsAlgorithmProvider) {
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

    public ColorDepthSearchAlgorithm<ColorDepthPixelMatchScore> createQueryColorDepthSearchWithDefaultThreshold(ImageArray<?> queryImage) {
        return cdsAlgorithmProvider.createColorDepthQuerySearchAlgorithmWithDefaultParams(queryImage, defaultQueryThreshold == null ? 0 : defaultQueryThreshold, 0);
    }

    public ColorDepthSearchAlgorithm<ColorDepthPixelMatchScore> createQueryColorDepthSearch(ImageArray<?> queryImage, int queryThreshold, int borderSize) {
        return cdsAlgorithmProvider.createColorDepthQuerySearchAlgorithmWithDefaultParams(queryImage, queryThreshold, borderSize);
    }


    public boolean isMatch(ColorDepthPixelMatchScore pixelMatchScore) {
        double pixMatchRatioThreshold = pctPositivePixels != null ? pctPositivePixels / 100 : 0.;
        return pixelMatchScore.getScore() > 0 && pixelMatchScore.getNormalizedScore() > pixMatchRatioThreshold;
    }

}
