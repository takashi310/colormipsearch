package org.janelia.colormipsearch.cmd;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
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

    @Parameter(names = {"--pixColorFluctuation"}, description = "Pix Color Fluctuation, 1.18 per slice")
    Double pixColorFluctuation = 2.0;

    @Parameter(names = {"--xyShift"}, description = "Number of pixels to try shifting in XY plane")
    Integer xyShift = 0;

    @Parameter(names = {"--negativeRadius"}, description = "Radius for gradient based score adjustment (negative radius)")
    int negativeRadius = 10;

    @Parameter(names = {"--mirrorMask"}, description = "Should the mask be mirrored across the Y axis?")
    boolean mirrorMask = false;

    @Parameter(names = {"--pctPositivePixels"}, description = "% of Positive PX Threshold (0-100%)")
    Double pctPositivePixels = 0.0;

    @Parameter(names = {"--librarySuffix"}, description = "Library suffix")
    String librarySuffix;

    @Parameter(names = {"--gradientPath", "-gp"}, description = "Gradient masks location")
    String gradientPath;

    @Parameter(names = {"--gradientSuffix"}, description = "Gradient suffix")
    String gradientSuffix = "_gradient";

    @Parameter(names = {"--zgapPath", "-zgp"}, description = "ZGap masks location")
    String zgapPath;

    @Parameter(names = {"--zgapSuffix"}, description = "ZGap suffix")
    String zgapSuffix;

    @Parameter(names = {"--libraryPartitionSize", "-lps"}, description = "Library partition size")
    int libraryPartitionSize = 100;

    @Parameter(names = {"--cdsConcurrency", "-cdc"}, description = "CDS concurrency - number of CDS tasks run concurrently")
    int cdsConcurrency;

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

}
