package org.janelia.colormipsearch;

import java.awt.Image;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.ZipFile;

import javax.annotation.Nullable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.colormipsearch.imageprocessing.ImageArray;
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
        @Parameter(names = "-h", description = "Display the help message", help = true, arity = 0)
        private boolean displayHelpMessage = false;
    }

    private static class CommonArgs {
        @Parameter(names = {"--outputDir", "-od"}, description = "Output directory")
        String outputDir;

        @Parameter(names = "-h", description = "Display the help message", help = true, arity = 0)
        boolean displayHelpMessage = false;
    }

    private static class AbstractArgs {
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

        @Parameter(names = {"--gradientPath", "-gp"}, description = "Gradient masks location")
        String gradientPath;

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

        AbstractArgs(CommonArgs commonArgs) {
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

        Set<String> filterAsLowerCase(Set<String> f) {
            if (CollectionUtils.isEmpty(f)) {
                return Collections.emptySet();
            } else {
                return f.stream().map(s -> s.toLowerCase()).collect(Collectors.toSet());
            }
        }
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

    enum SortingType {
        USE_MATCHING_SLICES_ONLY,
        WITH_GRADIENT_AREA_GAP
    }

    @Parameters(commandDescription = "Sort color depth search results")
    private static class SortResultsArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, description = "Results directory to be sorted")
        private String resultsDir;

        @Parameter(names = {"--resultsFile", "-rf"}, description = "File containing results to be sorted")
        private String resultsFile;

        @Parameter(names = {"--sortingType", "-st"}, description = "Sorting type")
        private SortingType sortingType = SortingType.WITH_GRADIENT_AREA_GAP;

        @ParametersDelegate
        final CommonArgs commonArgs;

        SortResultsArgs(CommonArgs commonArgs) {
            this.commonArgs = commonArgs;
        }

        Path getOutputDir() {
            if (StringUtils.isBlank(resultsDir) && StringUtils.isBlank(commonArgs.outputDir)) {
                return null;
            } else if (StringUtils.isBlank(resultsDir)) {
                return Paths.get(commonArgs.outputDir);
            } else {
                return Paths.get(resultsDir);
            }
        }
    }

    @Parameters(commandDescription = "Calculate gradient area score for the results")
    private static class GradientScoreResultsArgs extends AbstractArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, converter = ListArg.ListArgConverter.class, description = "Results directory to be sorted")
        private ListArg resultsDir;

        @Parameter(names = {"--resultsFile", "-rf"}, converter = ListArg.ListArgConverter.class, description = "File containing results to be sorted")
        private ListArg resultsFile;

        @Parameter(names = {"--topResults"}, description = "If set only calculate the gradient score for the top specified color depth search results")
        private int processTopResults;

        GradientScoreResultsArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }

        Path getOutputDir() {
            if (resultsDir == null && StringUtils.isBlank(commonArgs.outputDir)) {
                return null;
            } else if (StringUtils.isNotBlank(commonArgs.outputDir)) {
                return Paths.get(commonArgs.outputDir);
            } else {
                return Paths.get(resultsDir.input);
            }
        }
    }

    public static void main(String[] argv) {
        MainArgs mainArgs = new MainArgs();
        CommonArgs commonArgs = new CommonArgs();
        JsonMIPsSearchArgs jsonMIPsSearchArgs = new JsonMIPsSearchArgs(commonArgs);
        LocalMIPFilesSearchArgs localMIPFilesSearchArgs = new LocalMIPFilesSearchArgs(commonArgs);
        SortResultsArgs sortResultsArgs = new SortResultsArgs(commonArgs);
        GradientScoreResultsArgs gradientScoreResultsArgs = new GradientScoreResultsArgs(commonArgs);

        JCommander cmdline = JCommander.newBuilder()
                .addObject(mainArgs)
                .addCommand("searchFromJSON", jsonMIPsSearchArgs)
                .addCommand("searchLocalFiles", localMIPFilesSearchArgs)
                .addCommand("sortResults", sortResultsArgs)
                .addCommand("gradientScore", gradientScoreResultsArgs)
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

        switch (cmdline.getParsedCommand()) {
            case "searchFromJSON":
                createOutputDirs(jsonMIPsSearchArgs.getPerLibraryDir(), jsonMIPsSearchArgs.getPerMaskDir());
                runSearchFromJSONInput(jsonMIPsSearchArgs);
                break;
            case "searchLocalFiles":
                createOutputDirs(localMIPFilesSearchArgs.getPerLibraryDir(), localMIPFilesSearchArgs.getPerMaskDir());
                runSearchForLocalMIPFiles(localMIPFilesSearchArgs);
                break;
            case "gradientScore":
                if (gradientScoreResultsArgs.resultsDir == null && gradientScoreResultsArgs.resultsFile == null) {
                    StringBuilder sb = new StringBuilder("No result file or directory containing results has been specified").append('\n');
                    cmdline.usage(sb);
                    System.exit(1);
                }
                createOutputDirs(gradientScoreResultsArgs.getOutputDir());
                calculateGradientAreaScore(gradientScoreResultsArgs);
                break;
            case "sortResults":
                if (StringUtils.isBlank(sortResultsArgs.resultsDir) && StringUtils.isBlank(sortResultsArgs.resultsFile)) {
                    StringBuilder sb = new StringBuilder("No result file or directory containing results has been specified").append('\n');
                    cmdline.usage(sb);
                    System.exit(1);
                }
                createOutputDirs(sortResultsArgs.getOutputDir());
                sortResults(sortResultsArgs);
                break;
            default:
                StringBuilder sb = new StringBuilder("Invalid command\n");
                cmdline.usage(sb);
                JCommander.getConsole().println(sb.toString());
                System.exit(1);
        }
    }

    private static void createOutputDirs(Path... outputDirs) {
        for (Path outputDir : outputDirs) {
            if (outputDir != null) {
                try {
                    // create output directory
                    Files.createDirectories(outputDir);
                } catch (IOException e) {
                    LOG.error("Error creating output directory: {}", outputDir, e);
                    System.exit(1);
                }
            }
        }
    }

    private static Executor createCDSExecutor(AbstractArgs args) {
        if (args.cdsConcurrency > 0) {
            return Executors.newFixedThreadPool(
                    args.cdsConcurrency,
                    new ThreadFactoryBuilder()
                            .setNameFormat("CDSRUNNER-%d")
                            .setDaemon(true)
                            .setPriority(Thread.NORM_PRIORITY + 1)
                            .build());
        } else {
            return Executors.newWorkStealingPool();
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
                    createCDSExecutor(args));
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
                createCDSExecutor(args));
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

    private static void sortResults(SortResultsArgs args) {
        if (StringUtils.isNotBlank(args.resultsFile)) {
            sortResultsFile(args.resultsFile, args.sortingType, args.getOutputDir());
        } else if (StringUtils.isNotBlank(args.resultsDir)) {
            try {
                Files.find(Paths.get(args.resultsDir), 1, (p, fa) -> fa.isRegularFile())
                        .forEach(p -> sortResultsFile(p.toString(), args.sortingType, args.getOutputDir()));
            } catch (IOException e) {
                LOG.error("Error listing {}", args.resultsDir, e);
            }
        }
    }

    private static void sortResultsFile(String inputResultsFilename, SortingType sortingType, Path outputDir) {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            LOG.info("Reading {}", inputResultsFilename);
            File inputResultsFile = new File(inputResultsFilename);
            Results<List<ColorMIPSearchResultMetadata>> resultsFileContent = mapper.readValue(inputResultsFile, new TypeReference<Results<List<ColorMIPSearchResultMetadata>>>() {
            });
            if (CollectionUtils.isEmpty(resultsFileContent.results)) {
                LOG.error("No color depth search results found in {}", inputResultsFile);
                return;
            }
            LOG.info("Read {} entries from {}", resultsFileContent.results.size(), inputResultsFilename);
            sortCDSResults(resultsFileContent);
            writeCDSResultsToJSONFile(resultsFileContent, getOutputFile(outputDir, inputResultsFile), mapper);
        } catch (IOException e) {
            LOG.error("Error reading {}", inputResultsFilename, e);
            throw new UncheckedIOException(e);
        }
    }

    private static void calculateGradientAreaScore(GradientScoreResultsArgs args) {
        EM2LMAreaGapCalculator gradientBasedScoreAdjuster = new EM2LMAreaGapCalculator(args.maskThreshold, args.negativeRadius, args.mirrorMask);
        Executor executor = createCDSExecutor(args);
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        Path outputDir = args.getOutputDir();
        if (args.resultsFile != null) {
            File cdsResultsFile = new File(args.resultsFile.input);
            Results<List<ColorMIPSearchResultMetadata>> cdsResults = calculateGradientAreaScoreForResultsFile(
                    gradientBasedScoreAdjuster, cdsResultsFile, args.gradientPath, args.processTopResults, mapper, executor
            );
            writeCDSResultsToJSONFile(cdsResults, getOutputFile(outputDir, cdsResultsFile), mapper);
        } else if (args.resultsDir != null) {
            try {
                int from = Math.max(args.resultsDir.offset, 0);
                int length = args.resultsDir.length;
                List<String> resultFileNames = Files.find(Paths.get(args.resultsDir.input), 1, (p, fa) -> fa.isRegularFile())
                        .skip(from)
                        .map(p -> p.toString())
                        .collect(Collectors.toList());
                List<String> filesToProcess;
                if (length > 0 && length < resultFileNames.size()) {
                    filesToProcess = resultFileNames.subList(0, length);
                } else {
                    filesToProcess = resultFileNames;
                }
                filesToProcess
                        .forEach(fn -> {
                            File f = new File(fn);
                            Results<List<ColorMIPSearchResultMetadata>> cdsResults = calculateGradientAreaScoreForResultsFile(
                                    gradientBasedScoreAdjuster, f, args.gradientPath, args.processTopResults, mapper, executor
                            );
                            writeCDSResultsToJSONFile(cdsResults, getOutputFile(outputDir, f), mapper);
                        });
            } catch (IOException e) {
                LOG.error("Error listing {}", args.resultsDir, e);
            }
        }
    }

    private static Results<List<ColorMIPSearchResultMetadata>> calculateGradientAreaScoreForResultsFile(
            EM2LMAreaGapCalculator emlmAreaGapCalculator,
            File inputResultsFile,
            String gradientsLocation,
            int topResultsToProcess,
            ObjectMapper mapper,
            Executor executor) {
        class ColorMIPSearchResultMetadataWithMIP {
            ColorMIPSearchResultMetadata csr;
            MIPInfo matchedMIP;
        }
        Results<List<ColorMIPSearchResultMetadata>> resultsFileContent = readCDSResultsFromJSONFile(inputResultsFile, mapper);
        if (CollectionUtils.isEmpty(resultsFileContent.results)) {
            LOG.error("No color depth search results found in {}", inputResultsFile);
            return resultsFileContent;
        }
        LOG.info("Read {} entries from {}", resultsFileContent.results.size(), inputResultsFile);
        Map<MIPInfo, List<ColorMIPSearchResultMetadataWithMIP>> resultsGroupedById = resultsFileContent.results.stream()
                .collect(Collectors.groupingBy(csr -> {
                    MIPInfo mip = new MIPInfo();
                    mip.id = csr.id;
                    mip.archivePath = csr.imageArchivePath;
                    mip.imagePath = csr.imageName;
                    mip.type = csr.imageType;
                    return mip;
                }, Collectors.collectingAndThen(Collectors.toList(), r -> {
                    List<ColorMIPSearchResultMetadata> toProcess;
                    if (topResultsToProcess > 0 && topResultsToProcess < r.size()) {
                        r.sort(Comparator.comparingInt(ColorMIPSearchResultMetadata::getMatchingPixels).reversed());
                        toProcess = r.subList(0, topResultsToProcess);
                    } else {
                        toProcess = r;
                    }
                    List<ColorMIPSearchResultMetadataWithMIP> rWithMatchedMIPs = toProcess.stream()
                            .map(csr -> {
                                ColorMIPSearchResultMetadataWithMIP csrWithMIP = new ColorMIPSearchResultMetadataWithMIP();
                                csrWithMIP.csr = csr;
                                MIPInfo matchedMIP = new MIPInfo();
                                matchedMIP.archivePath = csr.matchedImageArchivePath;
                                matchedMIP.imagePath = csr.matchedImageName;
                                matchedMIP.type = csr.matchedImageType;
                                csrWithMIP.matchedMIP = matchedMIP;
                                return csrWithMIP;
                            })
                            .collect(Collectors.toList());
                    LOG.info("Prepare {} mips out of {}", rWithMatchedMIPs.size(), r.size());
                    return rWithMatchedMIPs;
                })));
        long startTime = System.currentTimeMillis();
        List<CompletableFuture<List<ColorMIPSearchResultMetadata>>> gradientAreaGapComputations =
                Streams.zip(
                        IntStream.range(0, Integer.MAX_VALUE).boxed(),
                        resultsGroupedById.entrySet().stream(),
                        (i, resultsEntry) -> ImmutablePair.of(i + 1, resultsEntry))
                        .map(resultsEntry -> {
                            LOG.info("Submit calculate gradient area scores for matches of entry# {} - {} from {}", resultsEntry.getLeft(), resultsEntry.getRight().getKey(), inputResultsFile);
                            long startTimeForCurrentEntry = System.currentTimeMillis();
                            CompletableFuture<BiFunction<MIPImage, MIPImage, Long>> gradientGapCalculatorPromise = CompletableFuture.supplyAsync(() -> {
                                LOG.info("Load image {}", resultsEntry.getRight().getKey());
                                MIPImage inputImage = CachedMIPsUtils.loadMIP(resultsEntry.getRight().getKey());
                                return emlmAreaGapCalculator.getGradientAreaCalculator(inputImage);
                            }, executor);
                            List<CompletableFuture<Long>> areaGapComputations = resultsEntry.getRight().getValue().stream()
                                    .map(csr -> gradientGapCalculatorPromise.thenApplyAsync(gradientGapCalculator -> {
                                        long startGapCalcTime = System.currentTimeMillis();
                                        MIPImage matchedImage = CachedMIPsUtils.loadMIP(csr.matchedMIP);
                                        MIPImage matchedGradientImage = CachedMIPsUtils.loadMIP(MIPsUtils.getGradientMIPInfo(csr.matchedMIP, gradientsLocation));
                                        LOG.debug("Loaded images for calculating area gap for {} - {} with {} in {}ms",
                                                resultsEntry.getRight().getKey(), matchedImage, matchedGradientImage, System.currentTimeMillis()-startGapCalcTime);
                                        if (matchedImage != null && matchedGradientImage != null) {
                                            LOG.debug("Calculate area gap for {} - {} with {}",
                                                    resultsEntry.getRight().getKey(), matchedImage, matchedGradientImage);
                                            try {
                                                return gradientGapCalculator.apply(matchedImage, matchedGradientImage);
                                            } finally {
                                                LOG.debug("Finished calculating area gap for {} - {} with {} in {}ms",
                                                        resultsEntry.getRight().getKey(), matchedImage, matchedGradientImage, System.currentTimeMillis()-startGapCalcTime);
                                            }
                                        } else {
                                            // this branch is really inefficient but the hope is to not do it this way
                                            MIPImage inputImage = CachedMIPsUtils.loadMIP(resultsEntry.getRight().getKey());
                                            MIPImage inputGradientImage = CachedMIPsUtils.loadMIP(
                                                    MIPsUtils.getGradientMIPInfo(resultsEntry.getRight().getKey(), gradientsLocation)
                                            );
                                            return emlmAreaGapCalculator.calculateGradientAreaAdjustment(inputImage, inputGradientImage, matchedImage, matchedGradientImage);
                                        }
                                    }, executor))
                                    .collect(Collectors.toList());
                            return CompletableFuture.allOf(areaGapComputations.toArray(new CompletableFuture<?>[0]))
                                    .thenApply(vr -> {
                                        LOG.info("Completed gradient area scores for {} matches of entry# {} - {} from {} in {}s",
                                                resultsEntry.getRight().getValue().size(), resultsEntry.getKey(), resultsEntry.getRight().getKey(), inputResultsFile, (System.currentTimeMillis()-startTimeForCurrentEntry)/1000.);
                                        Double maxPctPixelScore = resultsEntry.getRight().getValue().stream()
                                                .map(csr -> csr.csr.getMatchingPixelsPct())
                                                .max(Double::compare).orElse(null);
                                        LOG.info("Max pixel percentage score for matches and max pixel percentage of entry# {} - {} from {} -> {}",
                                                resultsEntry.getLeft(), resultsEntry.getRight().getKey(), inputResultsFile, maxPctPixelScore);
                                        List<Long> areaGaps = areaGapComputations.stream()
                                                .map(areaGapComputation -> areaGapComputation.join())
                                                .collect(Collectors.toList());
                                        long maxAreaGap = areaGaps.stream().max(Long::compare).orElse(-1L);
                                        LOG.info("Max area gap for matches and max pixel percentage of entry# {} - {} from {} -> {}",
                                                resultsEntry.getLeft(), resultsEntry.getRight().getKey(), inputResultsFile, maxAreaGap);
                                        return Streams.zip(
                                                resultsEntry.getRight().getValue().stream(),
                                                areaGaps.stream(),
                                                (csr, areaGap) -> {
                                                    csr.csr.setGradientAreaGap(areaGap);
                                                    if (maxAreaGap > 0 && areaGap >= 0) {
                                                        double normAreaGapScore = ((double) areaGap / maxAreaGap) * 2.5;
                                                        if (normAreaGapScore > 1) {
                                                            csr.csr.setNormalizedGapScore(1.);
                                                        } else {
                                                            csr.csr.setNormalizedGapScore(Math.max(normAreaGapScore, 0.002));
                                                        }
                                                        if (maxPctPixelScore != null) {
                                                            double normalizedGapScore = (csr.csr.getMatchingPixelsPct() / maxPctPixelScore) / csr.csr.getNormalizedGapScore() * 100.;
                                                            csr.csr.setNormalizedGapScore(normalizedGapScore);
                                                        } else {
                                                            csr.csr.setNormalizedGapScore(null);
                                                        }
                                                    } else {
                                                        csr.csr.setNormalizedGapScore(null);
                                                    }
                                                    return csr.csr;
                                                }
                                        ).collect(Collectors.toList());
                                    });
                        })
                        .collect(Collectors.toList());
        // wait for all results to complete
        CompletableFuture.allOf(gradientAreaGapComputations.toArray(new CompletableFuture<?>[0])).join();
        LOG.info("Finished gradient area score for {} entries from {} in {}s", gradientAreaGapComputations.size(), inputResultsFile, (System.currentTimeMillis() - startTime) / 1000.);
        sortCDSResults(resultsFileContent);
        return resultsFileContent;
    }

    private static void sortCDSResults(Results<List<ColorMIPSearchResultMetadata>> cdsResults) {
        Comparator<ColorMIPSearchResultMetadata> csrComp = (csr1, csr2) -> {
            if (csr1.getNormalizedGapScore() != null && csr2.getNormalizedGapScore() != null) {
                return Comparator.comparingDouble(ColorMIPSearchResultMetadata::getNormalizedGapScore)
                        .compare(csr1, csr2)
                        ;
            } else if (csr1.getNormalizedGapScore() == null && csr2.getNormalizedGapScore() == null) {
                return Comparator.comparingDouble(ColorMIPSearchResultMetadata::getMatchingPixelsPct)
                        .compare(csr1, csr2)
                        ;
            } else if (csr1.getNormalizedGapScore() == null) {
                // null gap scores should be at the beginning
                return -1;
            } else {
                return 1;
            }
        };
        cdsResults.results.sort(csrComp.reversed());
    }

    private static Results<List<ColorMIPSearchResultMetadata>> readCDSResultsFromJSONFile(File f, ObjectMapper mapper) {
        try {
            LOG.info("Reading {}", f);
            return mapper.readValue(f, new TypeReference<Results<List<ColorMIPSearchResultMetadata>>>() {
            });
        } catch (IOException e) {
            LOG.error("Error reading CDS results from json file {}", f, e);
            throw new UncheckedIOException(e);
        }
    }

    @Nullable
    private static File getOutputFile(Path outputDir, File inputFile) {
        if (outputDir == null) {
            return null;
        } else {
            return outputDir.resolve(inputFile.getName()).toFile();
        }
    }

    private static void writeCDSResultsToJSONFile(Results<List<ColorMIPSearchResultMetadata>> cdsResults, File f, ObjectMapper mapper) {
        try {
            if (CollectionUtils.isNotEmpty(cdsResults.results)) {
                if (f == null) {
                    mapper.writerWithDefaultPrettyPrinter().writeValue(System.out, cdsResults);
                } else {
                    LOG.info("Writing {}", f);
                    mapper.writerWithDefaultPrettyPrinter().writeValue(f, cdsResults);
                }
            }
        } catch (IOException e) {
            LOG.error("Error writing CDS results to json file {}", f, e);
            throw new UncheckedIOException(e);
        }
    }

}
