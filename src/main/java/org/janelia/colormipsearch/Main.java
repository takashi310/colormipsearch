package org.janelia.colormipsearch;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.ZipFile;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.colormipsearch.imageprocessing.TriFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static class MainArgs {
        @Parameter(names = "--cacheSize", description = "Max cache size")
        private long cacheSize = 200000L;
        @Parameter(names = "--cacheExpirationInMin", description = "Cache expiration in minutes")
        private long cacheExpirationInMin = 60;
        @Parameter(names = "-h", description = "Display the help message", help = true, arity = 0)
        private boolean displayHelpMessage = false;
    }

    @Parameters(commandDescription = "Color depth search for MIP files")
    private static class LocalMIPFilesSearchArgs extends AbstractArgs {
        @Parameter(names = "-i", required = true, converter = ListArg.ListArgConverter.class, description = "Library MIPs location")
        ListArg libraryMIPsLocation;

        @Parameter(names = "-m", required = true, converter = ListArg.ListArgConverter.class, description = "Mask MIPs location")
        ListArg maskMIPsLocation;

        LocalMIPFilesSearchArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }
    }

    @Parameters(commandDescription = "Color depth search for a batch of MIPs")
    private static class JsonMIPsSearchArgs extends AbstractArgs {
        @Parameter(names = {"--images", "-i"}, required = true, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "Comma-delimited list of directories containing images to search")
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


    public static void main(String[] argv) {
        MainArgs mainArgs = new MainArgs();
        CommonArgs commonArgs = new CommonArgs();
        JsonMIPsSearchArgs jsonMIPsSearchArgs = new JsonMIPsSearchArgs(commonArgs);
        LocalMIPFilesSearchArgs localMIPFilesSearchArgs = new LocalMIPFilesSearchArgs(commonArgs);
        CombineResultsCmd combineResultsCmd = new CombineResultsCmd(commonArgs);
        SetFakeGradientScoresCmd fakeGradientScoresCmd = new SetFakeGradientScoresCmd(commonArgs);
        CalculateGradientScoresCmd calculateGradientScoresCmd = new CalculateGradientScoresCmd(commonArgs);

        JCommander cmdline = JCommander.newBuilder()
                .addObject(mainArgs)
                .addCommand("searchFromJSON", jsonMIPsSearchArgs)
                .addCommand("searchLocalFiles", localMIPFilesSearchArgs)
                .addCommand("combineResults", combineResultsCmd.getArgs())
                .addCommand("gradientScore", calculateGradientScoresCmd.getArgs())
                .addCommand("initGradientScores", fakeGradientScoresCmd.getArgs())
                .build();

        try {
            cmdline.parse(argv);
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder(e.getMessage()).append('\n');
            if (StringUtils.isNotBlank(cmdline.getParsedCommand())) {
                cmdline.usage(cmdline.getParsedCommand(), sb);
            } else {
                cmdline.usage(sb);
            }
            JCommander.getConsole().println(sb.toString());
            System.exit(1);
        }

        if (mainArgs.displayHelpMessage) {
            cmdline.usage();
            System.exit(0);
        } else if (commonArgs.displayHelpMessage && StringUtils.isNotBlank(cmdline.getParsedCommand())) {
            cmdline.usage(cmdline.getParsedCommand());
            System.exit(0);
        } else if (StringUtils.isBlank(cmdline.getParsedCommand())) {
            StringBuilder sb = new StringBuilder("Missing command\n");
            cmdline.usage(sb);
            JCommander.getConsole().println(sb.toString());
            System.exit(1);
        }
        // initialize the cache
        CachedMIPsUtils.initializeCache(mainArgs.cacheSize, mainArgs.cacheExpirationInMin);
        // invoke the appropriate command
        switch (cmdline.getParsedCommand()) {
            case "searchFromJSON":
                CmdUtils.createOutputDirs(jsonMIPsSearchArgs.getPerLibraryDir(), jsonMIPsSearchArgs.getPerMaskDir());
                runSearchFromJSONInput(jsonMIPsSearchArgs);
                break;
            case "searchLocalFiles":
                CmdUtils.createOutputDirs(localMIPFilesSearchArgs.getPerLibraryDir(), localMIPFilesSearchArgs.getPerMaskDir());
                runSearchForLocalMIPFiles(localMIPFilesSearchArgs);
                break;
            case "gradientScore":
                if (calculateGradientScoresCmd.getArgs().getResultsDir() == null && calculateGradientScoresCmd.getArgs().getResultsFile() == null) {
                    StringBuilder sb = new StringBuilder("No result file or directory containing results has been specified").append('\n');
                    cmdline.usage(cmdline.getParsedCommand(), sb);
                    System.exit(1);
                }
                CmdUtils.createOutputDirs(calculateGradientScoresCmd.getArgs().getOutputDir());
                calculateGradientScoresCmd.execute();
                break;
            case "combineResults":
                if (CollectionUtils.isEmpty(combineResultsCmd.getArgs().resultsDirs) &&
                        CollectionUtils.isEmpty(combineResultsCmd.getArgs().resultsFiles)) {
                    StringBuilder sb = new StringBuilder("No result file or directory containing results has been specified").append('\n');
                    cmdline.usage(cmdline.getParsedCommand(), sb);
                    System.exit(1);
                }
                CmdUtils.createOutputDirs(combineResultsCmd.getArgs().getOutputDir());
                combineResultsCmd.execute();
                break;
            case "initGradientScores":
                if (CollectionUtils.isEmpty(fakeGradientScoresCmd.getArgs().resultsDirs) &&
                        CollectionUtils.isEmpty(fakeGradientScoresCmd.getArgs().resultsFiles)) {
                    StringBuilder sb = new StringBuilder("No result file or directory containing results has been specified").append('\n');
                    cmdline.usage(cmdline.getParsedCommand(), sb);
                    System.exit(1);
                }
                CmdUtils.createOutputDirs(fakeGradientScoresCmd.getArgs().getOutputDir());
                fakeGradientScoresCmd.execute();
                break;
            default:
                StringBuilder sb = new StringBuilder("Invalid command\n");
                cmdline.usage(sb);
                JCommander.getConsole().println(sb.toString());
                System.exit(1);
        }
    }

    private static void runSearchFromJSONInput(JsonMIPsSearchArgs args) {
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
                    .flatMap(libraryInput -> readMIPsFromJSON(libraryInput, args.filterAsLowerCase(args.libraryMIPsFilter)).stream())
                    .collect(Collectors.toList());

            List<MIPInfo> masksMips = args.masksInputs.stream()
                    .flatMap(masksInput -> readMIPsFromJSON(masksInput, args.filterAsLowerCase(args.maskMIPsFilter)).stream())
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

    private static void saveCDSParameters(ColorMIPSearch colorMIPSearch, Path outputDir, String fname) {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        if (outputDir != null) {
            File outputFile = outputDir.resolve(fname).toFile();
            try {
                mapper.writerWithDefaultPrettyPrinter().
                        writeValue(outputFile, colorMIPSearch.getCDSParameters());
            } catch (IOException e) {
                LOG.error("Error persisting color depth search parameters to {}", outputFile, e);
                throw new IllegalStateException(e);
            }
        }
    }

    private static List<MIPInfo> readMIPsFromJSON(ListArg mipsArg, Set<String> filter) {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            LOG.info("Reading {}", mipsArg);
            List<MIPInfo> content = mapper.readValue(new File(mipsArg.input), new TypeReference<List<MIPInfo>>() {
            });
            if (CollectionUtils.isEmpty(filter)) {
                int from = mipsArg.offset > 0 ? mipsArg.offset : 0;
                int to = mipsArg.length > 0 ? Math.min(from + mipsArg.length, content.size()) : content.size();
                LOG.info("Read {} mips from {} starting at {} to {}", content.size(), mipsArg, from, to);
                return content.subList(from, to);
            } else {
                LOG.info("Read {} from {} mips", filter, content.size());
                return content.stream()
                        .filter(mip -> filter.contains(mip.publishedName.toLowerCase()) || filter.contains(StringUtils.lowerCase(mip.id)))
                        .collect(Collectors.toList());
            }
        } catch (IOException e) {
            LOG.error("Error reading {}", mipsArg, e);
            throw new UncheckedIOException(e);
        }
    }

    private static void runSearchForLocalMIPFiles(LocalMIPFilesSearchArgs args) {
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

    private static List<MIPInfo> readMIPsFromLocalFiles(ListArg mipsArg, Set<String> mipsFilter) {
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

    private static List<MIPInfo> readMIPsFromZipArchive(String mipsArchive, Set<String> mipsFilter, int offset, int length) {
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

    private static boolean isImageFile(Path p) {
        return isImageFile(p.getFileName().toString());
    }

    private static boolean isImageFile(String fname) {
        int extseparator = fname.lastIndexOf('.');
        if (extseparator == -1) {
            return false;
        }
        String fext = fname.substring(extseparator + 1);
        switch (fext.toLowerCase()) {
            case "jpg":
            case "jpeg":
            case "png":
            case "tif":
            case "tiff":
                return true;
            default:
                return false;
        }
    }

}
