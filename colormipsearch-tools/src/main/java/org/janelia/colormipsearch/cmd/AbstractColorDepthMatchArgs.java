package org.janelia.colormipsearch.cmd;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;

import com.beust.jcommander.Parameter;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.imageprocessing.ImageRegionDefinition;

class AbstractColorDepthMatchArgs extends AbstractCmdArgs {
    @Parameter(names = "--app")
    String appName = "ColorMIPSearch";

    @Parameter(names = {"--dataThreshold"}, description = "Data threshold")
    Integer dataThreshold = 100;

    @Parameter(names = {"--maskThreshold"}, description = "Mask threshold")
    Integer maskThreshold = 100;

    @Parameter(names = {"--border"}, description = "Image border size where we know for sure there is no useful information")
    Integer borderSize = 0;

    @Parameter(names = {"--pixColorFluctuation"}, description = "Pix Color Fluctuation, 1.18 per slice")
    Double pixColorFluctuation = 2.0;

    @Parameter(names = {"--xyShift"}, description = "Number of pixels to try shifting in XY plane")
    Integer xyShift = 0;

    @Parameter(names = {"--negativeRadius"}, description = "Radius for gradient based score adjustment (negative radius)")
    int negativeRadius = 20;

    @Parameter(names = {"--mirrorMask"}, description = "Should the mask be mirrored across the Y axis?")
    boolean mirrorMask = false;

    @Parameter(names = {"--pctPositivePixels"}, description = "% of Positive PX Threshold (0-100%)")
    Double pctPositivePixels = 0.0;

    @Parameter(names = {"--with-grad-scores"}, description = "If possible calculate the negative scores as well", arity = 0)
    boolean withGradientScores = false;

    @Parameter(names = {"--librarySuffix"}, description = "Library suffix")
    String librarySuffix;

    @Parameter(names = {"--gradientVariant"}, description = "Gradient variant key")
    String gradientVariantKey = "gradient";

    @Parameter(names = {"--gradientPath", "-gp"}, description = "Gradient masks location", variableArity = true)
    List<String> gradientPaths;

    @Parameter(names = {"--gradientSuffix"}, description = "Gradient suffix")
    String gradientSuffix = "_gradient";

    @Parameter(names = {"--zgapVariant"}, description = "zgap variant key")
    String zgapVariantKey = "zgap";

    @Parameter(names = {"--zgapPath", "-zgp"}, description = "ZGap masks location", variableArity = true)
    List<String> zgapPaths;

    @Parameter(names = {"--zgapSuffix"}, description = "ZGap suffix")
    String zgapSuffix;

    @Parameter(names = {"--processingPartitionSize", "-ps", "--libraryPartitionSize"}, description = "Processing partition size")
    int processingPartitionSize = 100;

    @Parameter(names = {"--no-name-labels"},
            description = "If true the mips do not have the name labels so they do not need to be cleared",
            arity = 0)
    boolean noNameLabel = false;

    @Parameter(names = {"--no-colormap-labels"},
            description = "If true the mips do not have the color map labels so they do not need to be cleared",
            arity = 0)
    boolean noColorScaleLabel = false;

    @Parameter(names = {"--libraryFilter", "-lf"}, variableArity = true, description = "Filter for library mips")
    Set<String> libraryMIPsFilter;

    @Parameter(names = {"--masksFilter", "-mf"}, variableArity = true, description = "Filter for mask mips")
    Set<String> maskMIPsFilter;

    @Parameter(names = {"--perMaskSubdir"}, description = "Results subdirectory for results grouped by mask MIP ID")
    String perMaskSubdir;

    @Parameter(names = {"--perLibrarySubdir"}, description = "Results subdirectory for results grouped by library MIP ID")
    String perLibrarySubdir;

    @Parameter(names = {"--query-roi-mask"}, description = "Global ROI mask applied to all query images. " +
            "For example this could be the hemibrain mask when searching against hemibrain libraries.")
    String queryROIMaskName;

    AbstractColorDepthMatchArgs(CommonArgs commonArgs) {
        super(commonArgs);
    }

    Optional<Path> getPerMaskDir() {
        return getOutputDirArg()
                .map(dir -> StringUtils.isNotBlank(perMaskSubdir) ? dir.resolve(perMaskSubdir) : dir);
    }

    Optional<Path> getPerLibraryDir() {
        return getOutputDirArg()
                .map(dir -> StringUtils.isNotBlank(perLibrarySubdir) ? dir.resolve(perLibrarySubdir) : dir);
    }

    boolean onlyPositiveScores() {
        return !withGradientScores;
    }

    boolean hasNameLabel() {
        return !noNameLabel;
    }

    boolean hasColorScaleLabel() {
        return !noColorScaleLabel;
    }

    ImageRegionDefinition getRegionGeneratorForTextLabels() {
        // define the text label regions
        return img -> {
            int imgWidth = img.getWidth();
            BiPredicate<Integer, Integer> colorScaleLabelRegion;
            if (hasColorScaleLabel() && imgWidth > 270) {
                colorScaleLabelRegion = (x, y) -> x >= imgWidth - 270 && y < 90;
            } else {
                colorScaleLabelRegion = (x, y) -> false;
            }
            BiPredicate<Integer, Integer> nameLabelRegion;
            if (hasNameLabel()) {
                nameLabelRegion = (x, y) -> x < 330 && y < 100;
            } else {
                nameLabelRegion = (x, y) -> false;
            }
            return colorScaleLabelRegion.or(nameLabelRegion);
        };
    }

}
