package org.janelia.colormipsearch.api.cdsearch;

import java.io.Serializable;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.janelia.colormipsearch.api.cdmips.MIPImage;
import org.janelia.colormipsearch.api.cdmips.MIPsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class ColorMIPSearch implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ColorMIPSearch.class);

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

    public ColorDepthSearchAlgorithm<ColorMIPMatchScore> createMaskColorDepthSearch(MIPImage maskMIPImage, Integer maskThresholdParam) {
        ColorDepthSearchParams maskSpecificParams = new ColorDepthSearchParams().setParam("maskThreshold", maskThresholdParam);
        return cdsAlgorithmProvider.createColorDepthQuerySearchAlgorithm(maskMIPImage.getImageArray(), maskSpecificParams);
    }

    public boolean isMatch(ColorMIPMatchScore colorMIPMatchScore) {
        double pixMatchRatioThreshold = pctPositivePixels != null ? pctPositivePixels / 100 : 0.;
        return colorMIPMatchScore.getMatchingPixNumToMaskRatio() > pixMatchRatioThreshold;
    }

    public ColorMIPSearchResult runColorDepthSearch(MIPImage maskMIPImage,
                                                    MIPImage targetMIPImage,
                                                    MIPImage targetMIPGradientImage,
                                                    MIPImage targetMIPZGapMaskImage) {
        long startTime = System.currentTimeMillis();
        try {
            LOG.debug("Compare image mask {} with {}", maskMIPImage, targetMIPImage);
            ColorDepthSearchAlgorithm<ColorMIPMatchScore> maskColorDepthSearch = createMaskColorDepthSearch(maskMIPImage, null);
            ColorMIPMatchScore colorMIPMatchScore = maskColorDepthSearch.calculateMatchingScore(
                    MIPsUtils.getImageArray(targetMIPImage),
                    MIPsUtils.getImageArray(targetMIPGradientImage),
                    MIPsUtils.getImageArray(targetMIPZGapMaskImage));
            boolean isMatch = isMatch(colorMIPMatchScore);
            return new ColorMIPSearchResult(
                    MIPsUtils.getMIPMetadata(maskMIPImage),
                    MIPsUtils.getMIPMetadata(targetMIPImage),
                    colorMIPMatchScore,
                    isMatch,
                    false);
        } catch (Throwable e) {
            LOG.error("Error comparing mask {} with {}", maskMIPImage,  targetMIPImage, e);
            return new ColorMIPSearchResult(
                    MIPsUtils.getMIPMetadata(maskMIPImage),
                    MIPsUtils.getMIPMetadata(targetMIPImage),
                    ColorMIPMatchScore.NO_MATCH,
                    false,
                    true);
        } finally {
            LOG.debug("Completed comparing mask {} with {} in {}ms", maskMIPImage,  targetMIPImage, System.currentTimeMillis() - startTime);
        }
    }

    public Comparator<ColorMIPSearchResult> getColorMIPSearchResultComparator() {
        return Comparator.comparingInt(ColorMIPSearchResult::getMatchingPixels).reversed();
    }

}
