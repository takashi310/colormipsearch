package org.janelia.colormipsearch.cmd;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api.cdmips.MIPsUtils;
import org.janelia.colormipsearch.api.cdsearch.ColorDepthSearchAlgorithmProvider;
import org.janelia.colormipsearch.api.cdsearch.ColorDepthSearchAlgorithmProviderFactory;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPMatchScore;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearch;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResult;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResultUtils;
import org.janelia.colormipsearch.cmsdrivers.ColorMIPSearchDriver;
import org.janelia.colormipsearch.cmsdrivers.LocalColorMIPSearch;
import org.janelia.colormipsearch.utils.CachedMIPsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ColorDepthSearchLocalMIPsCmd extends AbstractColorDepthSearchCmd {
    private static final Logger LOG = LoggerFactory.getLogger(ColorDepthSearchLocalMIPsCmd.class);

    @Parameters(commandDescription = "Color depth search for MIP files")
    static class LocalMIPFilesSearchArgs extends AbstractColorDepthMatchArgs {
        @Parameter(names = "--search-name")
        String cdsSearchName;

        @Parameter(names = {"-m", "-q", "--queries"}, required = true, converter = ListArg.ListArgConverter.class, description = "Mask (or query) MIPs location")
        List<ListArg> maskImagesLocation;

        @Parameter(names = {"-i", "-t", "--targets"}, required = true, converter = ListArg.ListArgConverter.class, description = "Target MIPs location - this is typically the color depth library location")
        List<ListArg> searchableTargetImagesLocation;

        @Parameter(names = {"--viewableTargets"}, description = "location of the viewable images", variableArity = true)
        List<String> displayableImagesLocation;

        LocalMIPFilesSearchArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }
    }

    private final LocalMIPFilesSearchArgs args;
    private final Supplier<Long> cacheSizeSupplier;
    private final Supplier<Long> cacheExpirationInSecondsSupplier;

    ColorDepthSearchLocalMIPsCmd(String commandName,
                                 CommonArgs commonArgs,
                                 Supplier<Long> cacheSizeSupplier,
                                 Supplier<Long> cacheExpirationInSecondsSupplier) {
        super(commandName);
        this.args = new LocalMIPFilesSearchArgs(commonArgs);
        this.cacheSizeSupplier = cacheSizeSupplier;
        this.cacheExpirationInSecondsSupplier = cacheExpirationInSecondsSupplier;
    }

    @Override
    LocalMIPFilesSearchArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        CmdUtils.createOutputDirs(args.getPerLibraryDir(), args.getPerMaskDir());
        // initialize the cache
        CachedMIPsUtils.initializeCache(cacheSizeSupplier.get(), cacheExpirationInSecondsSupplier.get());
        runSearchForLocalMIPFiles(args);
    }

    private void runSearchForLocalMIPFiles(LocalMIPFilesSearchArgs args) {
        ColorDepthSearchAlgorithmProvider<ColorMIPMatchScore> cdsAlgorithmProvider;
        if (args.onlyPositiveScores()) {
            cdsAlgorithmProvider = ColorDepthSearchAlgorithmProviderFactory.createPixMatchCDSAlgorithmProvider(
                    args.maskThreshold,
                    args.mirrorMask,
                    args.dataThreshold,
                    args.pixColorFluctuation,
                    args.xyShift
            );
        } else {
            cdsAlgorithmProvider = ColorDepthSearchAlgorithmProviderFactory.createPixMatchWithNegativeScoreCDSAlgorithmProvider(
                    args.maskThreshold,
                    args.mirrorMask,
                    args.dataThreshold,
                    args.pixColorFluctuation,
                    args.xyShift,
                    args.negativeRadius,
                    loadQueryROIMask(args.queryROIMaskName)
            );
        }
        ColorMIPSearch colorMIPSearch = new ColorMIPSearch(args.pctPositivePixels, cdsAlgorithmProvider);
        ColorMIPSearchDriver colorMIPSearchDriver = new LocalColorMIPSearch(
                        colorMIPSearch,
                        args.libraryPartitionSize,
                        args.gradientPaths,
                        gradPathComponent -> {
                            String suffix = StringUtils.defaultIfBlank(args.gradientSuffix, "");
                            if (StringUtils.isNotBlank(args.librarySuffix)) {
                                return StringUtils.replaceIgnoreCase(gradPathComponent, args.librarySuffix, "") + suffix;
                            } else {
                                return gradPathComponent + suffix;
                            }
                        },
                        args.zgapPaths,
                        zgapPathComponent -> {
                            String suffix = StringUtils.defaultIfBlank(args.zgapSuffix, "");
                            if (StringUtils.isNotBlank(args.librarySuffix)) {
                                return StringUtils.replaceIgnoreCase(zgapPathComponent, args.librarySuffix, "") + suffix;
                            } else {
                                return zgapPathComponent + suffix;
                            }

                        },
                        CmdUtils.createCDSExecutor(args.commonArgs));
        try {
            List<MIPMetadata> queryMIPs = args.maskImagesLocation.stream()
                    .flatMap(masksLocation -> MIPsUtils.readMIPsFromLocalFiles(
                            masksLocation.input,
                            masksLocation.offset,
                            masksLocation.length,
                            CommonArgs.toLowerCase(args.maskMIPsFilter)
                    ).stream())
                    .collect(Collectors.toList());
            List<MIPMetadata> targetMIPs = args.searchableTargetImagesLocation.stream()
                    .flatMap(searchableTargetsLocation -> MIPsUtils.readMIPsFromLocalFiles(
                            searchableTargetsLocation.input,
                            searchableTargetsLocation.offset,
                            searchableTargetsLocation.length,
                            CommonArgs.toLowerCase(args.libraryMIPsFilter)
                    ).stream())
                    .collect(Collectors.toList());
            if (targetMIPs.isEmpty() || queryMIPs.isEmpty()) {
                LOG.warn("Both masks ({}) and targets ({}) must not be empty", queryMIPs.size(), targetMIPs.size());
            } else {
                saveCDSParameters(colorMIPSearch,
                        args.getBaseOutputDir(),
                        getCDSName(args.cdsSearchName, args.maskImagesLocation, args.searchableTargetImagesLocation));
                List<ColorMIPSearchResult> cdsResults = colorMIPSearchDriver.findAllColorDepthMatches(queryMIPs, targetMIPs);
                ColorMIPSearchResultsWriter.writeSearchResults(
                        args.getPerMaskDir(),
                        ColorMIPSearchResultUtils.groupResults(
                                cdsResults,
                                ColorMIPSearchResult::perMaskMetadata));
                if (StringUtils.isNotBlank(args.perLibrarySubdir)) {
                    ColorMIPSearchResultsWriter.writeSearchResults(
                            args.getPerLibraryDir(),
                            ColorMIPSearchResultUtils.groupResults(
                                    cdsResults,
                                    ColorMIPSearchResult::perLibraryMetadata));
                }
            }
        } finally {
            colorMIPSearchDriver.terminate();
        }
    }

    private String getCDSName(String searchName, List<ListArg> masks, List<ListArg> targets) {
        if (StringUtils.isNotBlank(searchName)) {
            return searchName;
        }
        String mask = masks.stream().map(arg -> arg.listArgName()).collect(Collectors.joining("+"));
        String target = targets.stream().map(arg -> arg.listArgName()).collect(Collectors.joining("+"));
        return mask + "-" + target + "-cdsparams.json";
    }
}
