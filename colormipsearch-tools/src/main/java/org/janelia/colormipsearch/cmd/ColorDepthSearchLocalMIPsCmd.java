package org.janelia.colormipsearch.cmd;

import java.util.List;
import java.util.function.Supplier;

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
        @Parameter(names = "-i", required = true, converter = ListArg.ListArgConverter.class, description = "Library MIPs location")
        ListArg libraryMIPsLocation;

        @Parameter(names = "-m", required = true, converter = ListArg.ListArgConverter.class, description = "Mask MIPs location")
        ListArg maskMIPsLocation;

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
        if (CollectionUtils.isNotEmpty(args.gradientPaths)) {
            cdsAlgorithmProvider = ColorDepthSearchAlgorithmProviderFactory.createPixMatchWithNegativeScoreCDSAlgorithmProvider(
                    args.maskThreshold,
                    args.mirrorMask,
                    args.dataThreshold,
                    args.pixColorFluctuation,
                    args.xyShift,
                    args.negativeRadius
            );
        } else {
            cdsAlgorithmProvider = ColorDepthSearchAlgorithmProviderFactory.createPixMatchCDSAlgorithmProvider(
                    args.maskThreshold,
                    args.mirrorMask,
                    args.dataThreshold,
                    args.pixColorFluctuation,
                    args.xyShift
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
            List<MIPMetadata> librariesMips = MIPsUtils.readMIPsFromLocalFiles(
                    args.libraryMIPsLocation.input,
                    args.libraryMIPsLocation.offset,
                    args.libraryMIPsLocation.length,
                    CommonArgs.toLowerCase(args.libraryMIPsFilter)
            );
            List<MIPMetadata> masksMips = MIPsUtils.readMIPsFromLocalFiles(
                    args.maskMIPsLocation.input,
                    args.maskMIPsLocation.offset,
                    args.maskMIPsLocation.length,
                    CommonArgs.toLowerCase(args.maskMIPsFilter)
            );
            if (librariesMips.isEmpty() || masksMips.isEmpty()) {
                LOG.warn("Both masks ({}) and libraries ({}) must not be empty", masksMips.size(), librariesMips.size());
            } else {
                String inputName = args.libraryMIPsLocation.listArgName();
                String maskName = args.maskMIPsLocation.listArgName();
                saveCDSParameters(colorMIPSearch, args.getBaseOutputDir(), "masks-" + maskName + "-inputs-" + inputName + "-cdsParameters.json");
                List<ColorMIPSearchResult> cdsResults = colorMIPSearchDriver.findAllColorDepthMatches(masksMips, librariesMips);
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

}
