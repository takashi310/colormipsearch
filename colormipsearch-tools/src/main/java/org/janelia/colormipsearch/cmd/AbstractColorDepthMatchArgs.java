package org.janelia.colormipsearch.cmd;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

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

    @ParametersDelegate
    final CommonArgs commonArgs;

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
        this.commonArgs = commonArgs;
    }

    Path getBaseOutputDir() {
        return StringUtils.isBlank(commonArgs.outputDir) ? null : Paths.get(commonArgs.outputDir);
    }

    Path getPerMaskDir() {
        if (StringUtils.isBlank(commonArgs.outputDir)) {
            return null;
        } else {
            if (StringUtils.isBlank(perMaskSubdir)) {
                return Paths.get(commonArgs.outputDir);
            } else {
                return Paths.get(commonArgs.outputDir, perMaskSubdir);
            }
        }
    }

    Path getPerLibraryDir() {
        if (StringUtils.isBlank(commonArgs.outputDir)) {
            return null;
        } else {
            if (StringUtils.isBlank(perLibrarySubdir)) {
                return Paths.get(commonArgs.outputDir);
            } else {
                return Paths.get(commonArgs.outputDir, perLibrarySubdir);
            }
        }
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
}
