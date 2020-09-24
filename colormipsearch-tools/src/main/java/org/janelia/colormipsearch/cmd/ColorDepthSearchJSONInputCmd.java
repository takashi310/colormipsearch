package org.janelia.colormipsearch.cmd;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

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
import org.janelia.colormipsearch.cmsdrivers.SparkColorMIPSearch;
import org.janelia.colormipsearch.utils.CachedMIPsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ColorDepthSearchJSONInputCmd extends AbstractColorDepthSearchCmd {
    private static final Logger LOG = LoggerFactory.getLogger(ColorDepthSearchJSONInputCmd.class);

    @Parameters(commandDescription = "Color depth search for a batch of MIPs")
    static class JsonMIPsSearchArgs extends AbstractColorDepthMatchArgs {
        @Parameter(names = {"--images", "-i"}, required = true, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "Comma-delimited list of JSON configs containing images to search")
        List<ListArg> librariesInputs;

        @Parameter(names = {"--images-index"}, description = "Input image file(s) start index")
        long librariesStartIndex;

        @Parameter(names = {"--images-length"}, description = "Input image file(s) length")
        int librariesLength;

        @Parameter(names = {"--masks", "-m"}, required = true, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "Image file(s) to use as the search masks")
        List<ListArg> masksInputs;

        @Parameter(names = {"--masks-index"}, description = "Mask file(s) start index")
        long masksStartIndex;

        @Parameter(names = {"--masks-length"}, description = "Mask file(s) length")
        int masksLength;

        @Parameter(names = "-useSpark", description = "Perform the search in the current process", arity = 0)
        boolean useSpark = false;

        JsonMIPsSearchArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }

        boolean useSpark() {
            return useSpark;
        }
    }

    private final JsonMIPsSearchArgs args;
    private final Supplier<Long> cacheSizeSupplier;
    private final Supplier<Long> cacheExpirationInSecondsSupplier;

    ColorDepthSearchJSONInputCmd(String commandName,
                                 CommonArgs commonArgs,
                                 Supplier<Long> cacheSizeSupplier,
                                 Supplier<Long> cacheExpirationInSecondsSupplier) {
        super(commandName);
        this.args = new JsonMIPsSearchArgs(commonArgs);
        this.cacheSizeSupplier = cacheSizeSupplier;
        this.cacheExpirationInSecondsSupplier = cacheExpirationInSecondsSupplier;
    }

    @Override
    JsonMIPsSearchArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        CmdUtils.createOutputDirs(args.getPerLibraryDir(), args.getPerMaskDir());
        // initialize the cache
        CachedMIPsUtils.initializeCache(cacheSizeSupplier.get(), cacheExpirationInSecondsSupplier.get());
        runColorDepthSearchFromJSONInput(args);
    }

    private void runColorDepthSearchFromJSONInput(JsonMIPsSearchArgs args) {
        ColorMIPSearchDriver colorMIPSearchDriver;
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
        if (args.useSpark()) {
            String librarySuffixArg = args.librarySuffix;
            String gradientSuffixArg = args.gradientSuffix;
            String zgapSuffixArg = args.zgapSuffix;
            colorMIPSearchDriver = new SparkColorMIPSearch(
                    args.appName,
                    colorMIPSearch,
                    args.gradientPaths,
                    gradPathComponent -> {
                        String suffix = StringUtils.defaultIfBlank(gradientSuffixArg, "");
                        if (StringUtils.isNotBlank(librarySuffixArg)) {
                            return StringUtils.replaceIgnoreCase(gradPathComponent, librarySuffixArg, "") + suffix;
                        } else {
                            return gradPathComponent + suffix;
                        }
                    },
                    args.zgapPaths,
                    zgapPathComponent -> {
                        String suffix = StringUtils.defaultIfBlank(zgapSuffixArg, "");
                        if (StringUtils.isNotBlank(librarySuffixArg)) {
                            return StringUtils.replaceIgnoreCase(zgapPathComponent, librarySuffixArg, "") + suffix;
                        } else {
                            return zgapPathComponent + suffix;
                        }
                    });
        } else {
            colorMIPSearchDriver = new LocalColorMIPSearch(
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
        }
        try {
            ObjectMapper mapper = new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            long librariesStartIndex = args.librariesStartIndex > 0 ? args.librariesStartIndex : 0;
            int librariesLength = args.librariesLength > 0 ? args.librariesLength : 0;
            List<MIPMetadata> inputLibrariesMips = args.librariesInputs.stream()
                    .flatMap(libraryInput -> MIPsUtils.readMIPsFromJSON(
                            libraryInput.input,
                            libraryInput.offset,
                            libraryInput.length,
                            CommonArgs.toLowerCase(args.libraryMIPsFilter),
                            mapper).stream())
                    .skip(librariesStartIndex)
                    .collect(Collectors.toList());

            List<MIPMetadata> librariesMips;
            if (librariesLength > 0 && librariesLength < inputLibrariesMips.size()) {
                librariesMips = inputLibrariesMips.subList(0, librariesLength);
            } else {
                librariesMips = inputLibrariesMips;
            }

            long masksStartIndex = args.masksStartIndex > 0 ? args.masksStartIndex : 0;
            int masksLength = args.masksLength > 0 ? args.masksLength : 0;
            List<MIPMetadata> inputMasksMips = args.masksInputs.stream()
                    .flatMap(masksInput -> MIPsUtils.readMIPsFromJSON(
                            masksInput.input,
                            masksInput.offset,
                            masksInput.length,
                            CommonArgs.toLowerCase(args.maskMIPsFilter),
                            mapper).stream())
                    .skip(masksStartIndex)
                    .collect(Collectors.toList());

            List<MIPMetadata> masksMips;
            if (masksLength > 0 && masksLength < inputMasksMips.size()) {
                masksMips = inputMasksMips.subList(0, masksLength);
            } else {
                masksMips = inputMasksMips;
            }

            if (librariesMips.isEmpty() || masksMips.isEmpty()) {
                LOG.warn("Both masks ({}) and libraries ({}) must not be empty", masksMips.size(), librariesMips.size());
            } else {
                String inputNames = args.librariesInputs.stream()
                        .map(ListArg::listArgName)
                        .reduce("", (l1, l2) -> StringUtils.isBlank(l1) ? l2 : l1 + "-" + l2);
                String maskNames = args.masksInputs.stream()
                        .map(ListArg::listArgName)
                        .reduce("", (l1, l2) -> StringUtils.isBlank(l1) ? l2 : l1 + "-" + l2);
                saveCDSParameters(colorMIPSearch, args.getBaseOutputDir(), "masks-" + maskNames + "-inputs-" + inputNames + "-cdsParameters.json");
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
