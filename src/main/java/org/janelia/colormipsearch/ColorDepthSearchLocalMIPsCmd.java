package org.janelia.colormipsearch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipFile;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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
        try {
            List<MIPInfo> librariesMips = readMIPsFromLocalFiles(args.libraryMIPsLocation, args.filterAsLowerCase(args.libraryMIPsFilter));
            List<MIPInfo> masksMips = readMIPsFromLocalFiles(args.maskMIPsLocation, args.filterAsLowerCase(args.maskMIPsFilter));
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

    private List<MIPInfo> readMIPsFromLocalFiles(ListArg mipsArg, Set<String> mipsFilter) {
        Path mipsInputPath = Paths.get(mipsArg.input);
        if (Files.isDirectory(mipsInputPath)) {
            // read mips from the specified folder
            int from = mipsArg.offset > 0 ? mipsArg.offset : 0;
            try {
                List<MIPInfo> mips = Files.find(mipsInputPath, 1, (p, fa) -> fa.isRegularFile())
                        .filter(p -> isImageFile(p))
                        .filter(p -> {
                            if (CollectionUtils.isEmpty(mipsFilter)) {
                                return true;
                            } else {
                                String fname = p.getFileName().toString();
                                int separatorIndex = StringUtils.indexOf(fname, '_');
                                if (separatorIndex == -1) {
                                    return true;
                                } else {
                                    return mipsFilter.contains(StringUtils.substring(fname, 0, separatorIndex).toLowerCase());
                                }
                            }
                        })
                        .skip(from)
                        .map(p -> {
                            String fname = p.getFileName().toString();
                            int extIndex = fname.lastIndexOf('.');
                            MIPInfo mipInfo = new MIPInfo();
                            mipInfo.id = extIndex == -1 ? fname : fname.substring(0, extIndex);
                            mipInfo.imagePath = mipsInputPath.toString();
                            return mipInfo;
                        })
                        .collect(Collectors.toList());
                if (mipsArg.length > 0 && mipsArg.length < mips.size()) {
                    return mips.subList(0, mipsArg.length);
                } else {
                    return mips;
                }
            } catch (IOException e) {
                LOG.error("Error reading content of {}", mipsArg, e);
                return Collections.emptyList();
            }
        } else if (Files.isRegularFile(mipsInputPath)) {
            // check if the input is an archive (right now only zip is supported)
            if (StringUtils.endsWithIgnoreCase(mipsArg.input, ".zip")) {
                // read mips from zip
                return readMIPsFromZipArchive(mipsArg.input, mipsFilter, mipsArg.offset, mipsArg.length);
            } else if (isImageFile(mipsInputPath)) {
                // treat the file as a single image file
                String fname = mipsInputPath.getFileName().toString();
                int extIndex = fname.lastIndexOf('.');
                MIPInfo mipInfo = new MIPInfo();
                mipInfo.id = extIndex == -1 ? fname : fname.substring(0, extIndex);
                mipInfo.imagePath = mipsInputPath.toString();
                return Collections.singletonList(mipInfo);
            } else {
                return Collections.emptyList();
            }
        } else {
            LOG.warn("Cannot traverse links for {}", mipsArg);
            return Collections.emptyList();
        }
    }

    private List<MIPInfo> readMIPsFromZipArchive(String mipsArchive, Set<String> mipsFilter, int offset, int length) {
        ZipFile archiveFile;
        try {
            archiveFile = new ZipFile(mipsArchive);
        } catch (IOException e) {
            LOG.error("Error opening the archive stream for {}", mipsArchive, e);
            return Collections.emptyList();
        }
        try {
            int from = offset > 0 ? offset : 0;
            List<MIPInfo> mips = archiveFile.stream()
                    .filter(ze -> isImageFile(ze.getName()))
                    .filter(ze -> {
                        if (CollectionUtils.isEmpty(mipsFilter)) {
                            return true;
                        } else {
                            String fname = Paths.get(ze.getName()).getFileName().toString();
                            int separatorIndex = StringUtils.indexOf(fname, '_');
                            if (separatorIndex == -1) {
                                return true;
                            } else {
                                return mipsFilter.contains(StringUtils.substring(fname, 0, separatorIndex).toLowerCase());
                            }
                        }
                    })
                    .skip(from)
                    .map(ze -> {
                        String fname = Paths.get(ze.getName()).getFileName().toString();
                        int extIndex = fname.lastIndexOf('.');
                        MIPInfo mipInfo = new MIPInfo();
                        mipInfo.id = extIndex == -1 ? fname : fname.substring(0, extIndex);
                        mipInfo.type = "zipEntry";
                        mipInfo.archivePath = mipsArchive;
                        mipInfo.cdmPath = ze.getName();
                        mipInfo.imagePath = ze.getName();
                        return mipInfo;
                    })
                    .collect(Collectors.toList());
            if (length > 0 && length < mips.size()) {
                return mips.subList(0, length);
            } else {
                return mips;
            }
        } finally {
            try {
                archiveFile.close();
            } catch (IOException ignore) {
            }
        }

    }

    private boolean isImageFile(Path p) {
        return isImageFile(p.getFileName().toString());
    }

}
