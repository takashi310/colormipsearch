package org.janelia.colormipsearch;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ColorDepthSearchJSONInputCmd extends AbstractColorDepthSearchCmd {
    private static final Logger LOG = LoggerFactory.getLogger(ColorDepthSearchJSONInputCmd.class);

    @Parameters(commandDescription = "Color depth search for a batch of MIPs")
    static class JsonMIPsSearchArgs extends AbstractArgs {
        @Parameter(names = {"--images", "-i"}, required = true, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "Comma-delimited list of JSON configs containing images to search")
        private List<ListArg> librariesInputs;

        @Parameter(names = {"--masks", "-m"}, required = true, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "Image file(s) to use as the search masks")
        private List<ListArg> masksInputs;

        @Parameter(names = "-useSpark", description = "Perform the search in the current process", arity = 0)
        private boolean useSpark = false;

        JsonMIPsSearchArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }

        boolean useSpark() {
            return useSpark;
        }
    }

    private final JsonMIPsSearchArgs args;

    ColorDepthSearchJSONInputCmd(CommonArgs commonArgs) {
        this.args = new JsonMIPsSearchArgs(commonArgs);
    }

    JsonMIPsSearchArgs getArgs() {
        return args;
    }

    void execute() {
        runSearchFromJSONInput(args);
    }

    private void runSearchFromJSONInput(JsonMIPsSearchArgs args) {
        ColorMIPSearch colorMIPSearch;
        if (args.useSpark()) {
            colorMIPSearch = new SparkColorMIPSearch(
                    args.appName, args.gradientPath, args.dataThreshold, args.maskThreshold, args.pixColorFluctuation, args.xyShift, args.negativeRadius, args.mirrorMask, args.pctPositivePixels
            );
        } else {
            colorMIPSearch = new LocalColorMIPSearch(
                    args.gradientPath,
                    args.dataThreshold,
                    args.maskThreshold,
                    args.pixColorFluctuation,
                    args.xyShift,
                    args.negativeRadius,
                    args.mirrorMask,
                    args.pctPositivePixels,
                    args.libraryPartitionSize,
                    CmdUtils.createCDSExecutor(args));
        }

        try {
            List<MIPInfo> librariesMips = args.librariesInputs.stream()
                    .flatMap(libraryInput -> CmdUtils.readMIPsFromJSON(libraryInput.input, libraryInput.offset,  libraryInput.length, args.filterAsLowerCase(args.libraryMIPsFilter)).stream())
                    .collect(Collectors.toList());

            List<MIPInfo> masksMips = args.masksInputs.stream()
                    .flatMap(masksInput -> CmdUtils.readMIPsFromJSON(masksInput.input, masksInput.offset, masksInput.length, args.filterAsLowerCase(args.maskMIPsFilter)).stream())
                    .collect(Collectors.toList());

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
                List<ColorMIPSearchResult> cdsResults = colorMIPSearch.findAllColorDepthMatches(masksMips, librariesMips);
                new PerMaskColorMIPSearchResultsWriter().writeSearchResults(args.getPerMaskDir(), cdsResults);
                new PerLibraryColorMIPSearchResultsWriter().writeSearchResults(args.getPerLibraryDir(), cdsResults);
            }
        } finally {
            colorMIPSearch.terminate();
        }
    }

}
