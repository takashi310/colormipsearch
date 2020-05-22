package org.janelia.colormipsearch;

import java.io.Serializable;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    public ColorMIPSearchResult runImageComparison(MIPImage libraryMIPImage, MIPImage maskMIPImage) {
        long startTime = System.currentTimeMillis();
        try {
            LOG.debug("Compare library file {} with mask {}", libraryMIPImage,  maskMIPImage);
            double pixfludub = pixColorFluctuation / 100;

            final ColorMIPMaskCompare cc = new ColorMIPMaskCompare(
                    maskMIPImage.imageArray,
                    maskThreshold,
                    mirrorMask,
                    null,
                    0,
                    mirrorMask,
                    dataThreshold,
                    pixfludub,
                    xyShift
            );
            ColorMIPMaskCompare.Output output = cc.runSearch(libraryMIPImage.imageArray);

            double pixThresdub = pctPositivePixels / 100;
            boolean isMatch = output.getMatchingPct() > pixThresdub;

            return new ColorMIPSearchResult(
                    maskMIPImage.mipInfo,
                    libraryMIPImage.mipInfo,
                    output.getMatchingPixNum(), output.getMatchingPct(), isMatch, false);
        } catch (Throwable e) {
            LOG.warn("Error comparing library file {} with mask {}", libraryMIPImage,  maskMIPImage, e);
            return new ColorMIPSearchResult(
                    maskMIPImage != null ? maskMIPImage.mipInfo : null,
                    libraryMIPImage != null ? libraryMIPImage.mipInfo : null,
                    0, 0, false, true);
        } finally {
            LOG.debug("Completed comparing library file {} with mask {} in {}ms", libraryMIPImage,  maskMIPImage, System.currentTimeMillis() - startTime);
        }
    }

    public Comparator<ColorMIPSearchResult> getColorMIPSearchComparator() {
        return Comparator.comparingInt(ColorMIPSearchResult::getMatchingPixels).reversed();
    }

}
