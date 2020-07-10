package org.janelia.colormipsearch.cmd;

import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.cmsdrivers.ColorMIPSearchDriver;
import org.janelia.colormipsearch.cmsdrivers.LocalColorMIPSearch;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearch;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResult;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResultUtils;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api.cdmips.MIPsUtils;
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
        ColorMIPSearch colorMIPSearch = new ColorMIPSearch(
                args.maskThreshold,
                args.dataThreshold,
                args.pixColorFluctuation,
                args.xyShift,
                args.mirrorMask,
                args.pctPositivePixels);
        ColorMIPSearchDriver colorMIPSearchDriver = new LocalColorMIPSearch(colorMIPSearch, args.libraryPartitionSize, CmdUtils.createCDSExecutor(args));
        try {
            List<MIPMetadata> librariesMips = MIPsUtils.readMIPsFromLocalFiles(
                    args.libraryMIPsLocation.input, args.libraryMIPsLocation.offset, args.libraryMIPsLocation.length, args.filterAsLowerCase(args.libraryMIPsFilter)
            );
            List<MIPMetadata> masksMips = MIPsUtils.readMIPsFromLocalFiles(
                    args.maskMIPsLocation.input, args.maskMIPsLocation.offset, args.maskMIPsLocation.length, args.filterAsLowerCase(args.maskMIPsFilter)
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
