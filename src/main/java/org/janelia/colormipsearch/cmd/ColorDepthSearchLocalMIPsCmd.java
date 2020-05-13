package org.janelia.colormipsearch.cmd;

import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.ColorMIPSearchResult;
import org.janelia.colormipsearch.LocalColorMIPSearch;
import org.janelia.colormipsearch.MIPInfo;
import org.janelia.colormipsearch.MIPsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ColorDepthSearchLocalMIPsCmd extends AbstractColorDepthSearchCmd {
    private static final Logger LOG = LoggerFactory.getLogger(ColorDepthSearchLocalMIPsCmd.class);

    @Parameters(commandDescription = "Color depth search for MIP files")
    static class LocalMIPFilesSearchArgs extends AbstractArgs {
        @Parameter(names = "-i", required = true, converter = ListArg.ListArgConverter.class, description = "Library MIPs location")
        ListArg libraryMIPsLocation;

        @Parameter(names = "-m", required = true, converter = ListArg.ListArgConverter.class, description = "Mask MIPs location")
        ListArg maskMIPsLocation;

        LocalMIPFilesSearchArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }
    }

    private final LocalMIPFilesSearchArgs args;

    ColorDepthSearchLocalMIPsCmd(CommonArgs commonArgs) {
        this.args = new LocalMIPFilesSearchArgs(commonArgs);
    }

    LocalMIPFilesSearchArgs getArgs() {
        return args;
    }

    void execute() {
        runSearchForLocalMIPFiles(args);
    }

    private void runSearchForLocalMIPFiles(LocalMIPFilesSearchArgs args) {
        LocalColorMIPSearch colorMIPSearch = new LocalColorMIPSearch(
                args.dataThreshold,
                args.maskThreshold,
                args.pixColorFluctuation,
                args.xyShift,
                args.negativeRadius,
                args.mirrorMask,
                args.pctPositivePixels,
                args.libraryPartitionSize,
                args.gradientPath,
                args.gradientSuffix,
                CmdUtils.createCDSExecutor(args));
        try {
            List<MIPInfo> librariesMips = MIPsUtils.readMIPsFromLocalFiles(
                    args.libraryMIPsLocation.input, args.libraryMIPsLocation.offset, args.libraryMIPsLocation.length, args.filterAsLowerCase(args.libraryMIPsFilter)
            );
            List<MIPInfo> masksMips = MIPsUtils.readMIPsFromLocalFiles(
                    args.maskMIPsLocation.input, args.maskMIPsLocation.offset, args.maskMIPsLocation.length, args.filterAsLowerCase(args.maskMIPsFilter)
            );
            if (librariesMips.isEmpty() || masksMips.isEmpty()) {
                LOG.warn("Both masks ({}) and libraries ({}) must not be empty", masksMips.size(), librariesMips.size());
            } else {
                String inputName = args.libraryMIPsLocation.listArgName();
                String maskName = args.maskMIPsLocation.listArgName();
                saveCDSParameters(colorMIPSearch, args.getBaseOutputDir(), "masks-" + maskName + "-inputs-" + inputName + "-cdsParameters.json");
                List<ColorMIPSearchResult> cdsResults = colorMIPSearch.findAllColorDepthMatches(masksMips, librariesMips);
                new PerMaskColorMIPSearchResultsWriter().writeSearchResults(args.getPerMaskDir(), cdsResults);
                if (StringUtils.isNotBlank(args.perLibrarySubdir)) {
                    new PerLibraryColorMIPSearchResultsWriter().writeSearchResults(args.getPerLibraryDir(), cdsResults);
                }
            }
        } finally {
            colorMIPSearch.terminate();
        }
    }

}
