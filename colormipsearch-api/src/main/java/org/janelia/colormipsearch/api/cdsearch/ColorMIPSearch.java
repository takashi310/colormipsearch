package org.janelia.colormipsearch.api.cdsearch;

import java.io.Serializable;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.janelia.colormipsearch.api.cdmips.MIPImage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class ColorMIPSearch implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ColorMIPSearch.class);

    private final Integer defaultMaskThreshold;
    private final Integer dataThreshold;
    private final Integer xyShift;
    private final boolean mirrorMask;
    private final Double pixColorFluctuation;
    private final Double pctPositivePixels;

    public ColorMIPSearch(Integer defaultMaskThreshold,
                          Integer dataThreshold,
                          Double pixColorFluctuation,
                          Integer xyShift,
                          boolean mirrorMask,
                          Double pctPositivePixels) {
        this.defaultMaskThreshold = defaultMaskThreshold;
        this.dataThreshold = dataThreshold;
        this.pixColorFluctuation = pixColorFluctuation;
        this.xyShift = xyShift;
        this.mirrorMask = mirrorMask;
        this.pctPositivePixels = pctPositivePixels;
    }

    public Map<String, String> getCDSParameters() {
        Map<String, String> cdsParams = new LinkedHashMap<>();
        cdsParams.put("defaultMaskThreshold", defaultMaskThreshold != null ? defaultMaskThreshold.toString() : null);
        cdsParams.put("dataThreshold", dataThreshold != null ? dataThreshold.toString() : null);
        cdsParams.put("xyShift", xyShift != null ? xyShift.toString() : null);
        cdsParams.put("mirrorMask", String.valueOf(mirrorMask));
        cdsParams.put("pixColorFluctuation", pixColorFluctuation != null ? pixColorFluctuation.toString() : null);
        cdsParams.put("pctPositivePixels", pctPositivePixels != null ? pctPositivePixels.toString() : null);
        return cdsParams;
    }

    public ColorMIPMaskCompare createMaskComparatorWithDefaultThreshold(MIPImage maskMIPImage) {
        return createMaskComparator(maskMIPImage, defaultMaskThreshold);
    }

    public ColorMIPMaskCompare createMaskComparator(MIPImage maskMIPImage, Integer maskThresholdParam) {
        double zTolerance = pixColorFluctuation == null ? 0. : pixColorFluctuation / 100;
        int maskThreshold = maskThresholdParam != null
                ? maskThresholdParam
                : defaultMaskThreshold == null || defaultMaskThreshold < 0 ? 0 : defaultMaskThreshold;
        return ColorMIPMaskCompareFactory.createMaskComparator(
                maskMIPImage.getImageArray(),
                maskThreshold,
                mirrorMask,
                dataThreshold,
                zTolerance,
                xyShift
        );
    }

    public ColorMIPCompareOutput runImageComparison(ColorMIPMaskCompare maskComparator, MIPImage searchedMIPImage) {
        long startTime = System.currentTimeMillis();
        try {
            LOG.debug("Compare image file {}", searchedMIPImage);
            return maskComparator.runSearch(searchedMIPImage.getImageArray());
        } catch (Throwable e) {
            LOG.warn("Error comparing image file {}", searchedMIPImage, e);
            return ColorMIPCompareOutput.NO_MATCH;
        } finally {
            LOG.debug("Completed comparing image file {}in {}ms", searchedMIPImage,  System.currentTimeMillis() - startTime);
        }
    }

    public boolean isMatch(ColorMIPCompareOutput colorMIPCompareOutput) {
        double pixMatchRatioThreshold = pctPositivePixels != null ? pctPositivePixels / 100 : 0.;
        return colorMIPCompareOutput.getMatchingPixNumToMaskRatio() > pixMatchRatioThreshold;
    }

    public ColorMIPSearchResult runImageComparison(MIPImage searchedMIPImage, MIPImage maskMIPImage) {
        long startTime = System.currentTimeMillis();
        try {
            LOG.debug("Compare image file {} with mask {}", searchedMIPImage,  maskMIPImage);
            ColorMIPMaskCompare cc = createMaskComparatorWithDefaultThreshold(maskMIPImage);
            ColorMIPCompareOutput colorMIPCompareOutput = cc.runSearch(searchedMIPImage.getImageArray());
            boolean isMatch = isMatch(colorMIPCompareOutput);
            return new ColorMIPSearchResult(
                    maskMIPImage.getMipInfo(),
                    searchedMIPImage.getMipInfo(),
                    colorMIPCompareOutput.getMatchingPixNum(),
                    colorMIPCompareOutput.getMatchingPixNumToMaskRatio(),
                    isMatch,
                    false);
        } catch (Throwable e) {
            LOG.warn("Error comparing library file {} with mask {}", searchedMIPImage,  maskMIPImage, e);
            return new ColorMIPSearchResult(
                    maskMIPImage != null ? maskMIPImage.getMipInfo() : null,
                    searchedMIPImage != null ? searchedMIPImage.getMipInfo() : null,
                    0, 0, false, true);
        } finally {
            LOG.debug("Completed comparing library file {} with mask {} in {}ms", searchedMIPImage,  maskMIPImage, System.currentTimeMillis() - startTime);
        }
    }

    public Comparator<ColorMIPSearchResult> getColorMIPSearchResultComparator() {
        return Comparator.comparingInt(ColorMIPSearchResult::getMatchingPixels).reversed();
    }

}
