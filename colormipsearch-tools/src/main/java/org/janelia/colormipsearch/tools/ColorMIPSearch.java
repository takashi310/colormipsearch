package org.janelia.colormipsearch.tools;

import java.io.Serializable;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.janelia.colormipsearch.api.ColorMIPCompareOutput;
import org.janelia.colormipsearch.api.ColorMIPMaskCompare;
import org.janelia.colormipsearch.api.ColorMIPMaskCompareFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class ColorMIPSearch implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ColorMIPSearch.class);

    private final Integer dataThreshold;
    private final Integer maskThreshold;
    private final Integer xyShift;
    private final boolean mirrorMask;
    private final Double pixColorFluctuation;
    private final Double pctPositivePixels;

    public ColorMIPSearch(Integer dataThreshold,
                          Integer maskThreshold,
                          Double pixColorFluctuation,
                          Integer xyShift,
                          boolean mirrorMask,
                          Double pctPositivePixels) {
        this.dataThreshold = dataThreshold;
        this.maskThreshold = maskThreshold;
        this.pixColorFluctuation = pixColorFluctuation;
        this.xyShift = xyShift;
        this.mirrorMask = mirrorMask;
        this.pctPositivePixels = pctPositivePixels;
    }

    public Map<String, String> getCDSParameters() {
        Map<String, String> cdsParams = new LinkedHashMap<>();
        cdsParams.put("dataThreshold", dataThreshold != null ? dataThreshold.toString() : null);
        cdsParams.put("maskThreshold", maskThreshold != null ? maskThreshold.toString() : null);
        cdsParams.put("xyShift", xyShift != null ? xyShift.toString() : null);
        cdsParams.put("mirrorMask", String.valueOf(mirrorMask));
        cdsParams.put("pixColorFluctuation", pixColorFluctuation != null ? pixColorFluctuation.toString() : null);
        cdsParams.put("pctPositivePixels", pctPositivePixels != null ? pctPositivePixels.toString() : null);
        return cdsParams;
    }

    public ColorMIPMaskCompare createMaskComparator(MIPImage maskMIPImage) {
        double zTolerance = pixColorFluctuation == null ? 0. : pixColorFluctuation / 100;
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
            return maskComparator.runSearch(searchedMIPImage.imageArray);
        } catch (Throwable e) {
            LOG.warn("Error comparing image file {}", searchedMIPImage, e);
            return new ColorMIPCompareOutput(0, 0);
        } finally {
            LOG.debug("Completed comparing image file {}in {}ms", searchedMIPImage,  System.currentTimeMillis() - startTime);
        }
    }

    public boolean isMatch(ColorMIPCompareOutput colorMIPCompareOutput) {
        double pixThresdub = pctPositivePixels / 100;
        double pixMatchRatioThreshold = pctPositivePixels != null ? pctPositivePixels / 100 : 0.;
        return colorMIPCompareOutput.getMatchingPct() > pixMatchRatioThreshold;
    }

    public ColorMIPSearchResult runImageComparison(MIPImage searchedMIPImage, MIPImage maskMIPImage) {
        long startTime = System.currentTimeMillis();
        try {
            LOG.debug("Compare image file {} with mask {}", searchedMIPImage,  maskMIPImage);
            ColorMIPMaskCompare cc = createMaskComparator(maskMIPImage);
            ColorMIPCompareOutput colorMIPCompareOutput = cc.runSearch(searchedMIPImage.imageArray);

            double pixThresdub = pctPositivePixels / 100;
            boolean isMatch = colorMIPCompareOutput.getMatchingPct() > pixThresdub;

            return new ColorMIPSearchResult(
                    maskMIPImage.mipInfo,
                    searchedMIPImage.mipInfo,
                    colorMIPCompareOutput.getMatchingPixNum(), colorMIPCompareOutput.getMatchingPct(), isMatch, false);
        } catch (Throwable e) {
            LOG.warn("Error comparing library file {} with mask {}", searchedMIPImage,  maskMIPImage, e);
            return new ColorMIPSearchResult(
                    maskMIPImage != null ? maskMIPImage.mipInfo : null,
                    searchedMIPImage != null ? searchedMIPImage.mipInfo : null,
                    0, 0, false, true);
        } finally {
            LOG.debug("Completed comparing library file {} with mask {} in {}ms", searchedMIPImage,  maskMIPImage, System.currentTimeMillis() - startTime);
        }
    }

    public Comparator<ColorMIPSearchResult> getColorMIPSearchResultComparator() {
        return Comparator.comparingInt(ColorMIPSearchResult::getMatchingPixels).reversed();
    }

}
