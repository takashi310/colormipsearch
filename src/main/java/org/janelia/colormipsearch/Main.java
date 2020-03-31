package org.janelia.colormipsearch;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.ZipFile;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Streams;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final String CDS_PARAMETERS_FILE = "cdsParameters.json";

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

        AbstractArgs(CommonArgs commonArgs) {
            this.commonArgs = commonArgs;
        }

        String getOutputDir() {
            return commonArgs.outputDir;
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

        String getOutputDir() {
            return StringUtils.defaultIfBlank(commonArgs.outputDir, resultsDir);
        }
    }

    @Parameters(commandDescription = "Calculate gradient area score for the results")
    private static class GradientScoreResultsArgs extends AbstractArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, converter = ListArg.ListArgConverter.class, description = "Results directory to be sorted")
        private ListArg resultsDir;

        @Parameter(names = {"--resultsFile", "-rf"}, converter = ListArg.ListArgConverter.class, description = "File containing results to be sorted")
        private ListArg resultsFile;

        GradientScoreResultsArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }

        String getOutputDir() {
            if (resultsDir != null)
                return StringUtils.defaultIfBlank(commonArgs.outputDir, resultsDir.input);
            else
                return commonArgs.outputDir;
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
                createOutputDir(jsonMIPsSearchArgs.getOutputDir());
                runSearchFromJSONInput(jsonMIPsSearchArgs);
                break;
            case "searchLocalFiles":
                createOutputDir(localMIPFilesSearchArgs.getOutputDir());
                runSearchForLocalMIPFiles(localMIPFilesSearchArgs);
                break;
            case "gradientScore":
                if (gradientScoreResultsArgs.resultsDir == null && gradientScoreResultsArgs.resultsFile == null) {
                    StringBuilder sb = new StringBuilder("No result file or directory containing results has been specified").append('\n');
                    cmdline.usage(sb);
                    System.exit(1);
                }
                createOutputDir(gradientScoreResultsArgs.getOutputDir());
                calculateGradientAreaScore(gradientScoreResultsArgs);
                break;
            case "sortResults":
                if (StringUtils.isBlank(sortResultsArgs.resultsDir) && StringUtils.isBlank(sortResultsArgs.resultsFile)) {
                    StringBuilder sb = new StringBuilder("No result file or directory containing results has been specified").append('\n');
                    cmdline.usage(sb);
                    System.exit(1);
                }
                createOutputDir(sortResultsArgs.getOutputDir());
                sortResults(sortResultsArgs);
                break;
            default:
                StringBuilder sb = new StringBuilder("Invalid command\n");
                cmdline.usage(sb);
                JCommander.getConsole().println(sb.toString());
                System.exit(1);
        }
    }

    private static void createOutputDir(String outputDir) {
        if (StringUtils.isNotBlank(outputDir)) {
            try {
                // create output directory
                Files.createDirectories(Paths.get(outputDir));
            } catch (IOException e) {
                LOG.error("Error creating output directory: {}", outputDir, e);
                System.exit(1);
            }
        }
    }

    private static Executor createCDSExecutor(AbstractArgs args) {
        if (args.cdsConcurrency > 0) {
            return Executors.newWorkStealingPool(args.cdsConcurrency);
        } else {
            return Executors.newWorkStealingPool();
        }
    }

    private static void runSearchFromJSONInput(JsonMIPsSearchArgs args) {
        ColorMIPSearch colorMIPSearch;
        if (args.useSpark()) {
            colorMIPSearch = new SparkColorMIPSearch(
                    args.appName, args.gradientPath, args.getOutputDir(), args.dataThreshold, args.maskThreshold, args.pixColorFluctuation, args.xyShift, args.negativeRadius, args.mirrorMask, args.pctPositivePixels
            );
        } else {
            colorMIPSearch = new LocalColorMIPSearch(
                    args.gradientPath,
                    args.getOutputDir(),
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
                saveCDSParameters(colorMIPSearch, args.getOutputDir(), "cdsParameters.json");
                colorMIPSearch.compareEveryMaskWithEveryLibrary(masksMips, librariesMips);
            }
        } finally {
            colorMIPSearch.terminate();
        }
    }

    private static void saveCDSParameters(ColorMIPSearch colorMIPSearch, String outputDir, String fname) {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        File outputFile = new File(outputDir, fname);
        try {
            mapper.writerWithDefaultPrettyPrinter().
                    writeValue(outputFile, colorMIPSearch.getCDSParameters());
        } catch (IOException e) {
            LOG.error("Error persisting color depth search parameters to {}", outputFile, e);
            throw new IllegalStateException(e);
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
                args.getOutputDir(),
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
                saveCDSParameters(colorMIPSearch, args.getOutputDir(), CDS_PARAMETERS_FILE);
                colorMIPSearch.compareEveryMaskWithEveryLibrary(masksMips, librariesMips);

                List<ColorMIPSearchResult> cdsResults = colorMIPSearch.findAllColorDepthMatches(masksMips, librariesMips);
                // group the results by mask
                Map<String, List<ColorMIPSearchResult>> cdsResultsByMasks = cdsResults.stream()
                        .collect(Collectors.groupingBy(
                                ColorMIPSearchResult::getMaskId,
                                Collectors.collectingAndThen(
                                        Collectors.toList(),
                                        l -> {
                                            l.sort(Comparator.comparing(ColorMIPSearchResult::getMatchingPixels));
                                            return l;
                                        })));

                LOG.info("Write {} results by mask", cdsResultsByMasks.size());
                cdsResultsByMasks
                        .forEach((maskId, srsForCurrentMask) -> colorMIPSearch.writeSearchResults(
                                maskId,
                                srsForCurrentMask.stream().map(ColorMIPSearchResult::perMaskMetadata).collect(Collectors.toList())));
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
                MIPInfo mipInfo = new MIPInfo();
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
        String outputDir = args.getOutputDir();
        if (StringUtils.isNotBlank(args.resultsFile)) {
            sortResultsFile(args.resultsFile, args.sortingType, outputDir);
        } else if (StringUtils.isNotBlank(args.resultsDir)) {
            try {
                Files.find(Paths.get(args.resultsDir), 1, (p, fa) -> fa.isRegularFile())
                        .forEach(p -> sortResultsFile(p.toString(), args.sortingType, outputDir));
            } catch (IOException e) {
                LOG.error("Error listing {}", args.resultsDir, e);
            }
        }
    }

    private static void sortResultsFile(String inputResultsFilename, SortingType sortingType, String outputDir) {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            LOG.info("Reading {}", inputResultsFilename);
            File inputResultsFile = new File(inputResultsFilename);
            Results<List<ColorMIPSearchResultMetadata>> resultsFileContent = mapper.readValue(inputResultsFile, new TypeReference<Results<List<ColorMIPSearchResultMetadata>>>() {});
            if (CollectionUtils.isEmpty(resultsFileContent.results)) {
                LOG.error("No color depth search results found in {}", inputResultsFile);
                return;
            }
            LOG.info("Finished reading {} entries from {}", resultsFileContent.results.size(), inputResultsFilename);
            long maxAreaGap = resultsFileContent.results.stream()
                    .mapToLong(ColorMIPSearchResultMetadata::getGradientAreaGap)
                    .filter(a -> a != -1)
                    .max()
                    .orElse(-1);
            Comparator<ColorMIPSearchResultMetadata> srComparator;
            switch (sortingType) {
                case WITH_GRADIENT_AREA_GAP:
                    if (maxAreaGap == -1) {
                        srComparator = Comparator.comparing(ColorMIPSearchResultMetadata::getMatchingPixels);
                    } else {
                        srComparator = (sr1, sr2) -> {
                            // this is completely empirical because I don't know
                            // how to compare the results that have no area gap with the ones that have
                            long a1 = sr1.getGradientAreaGap();
                            long a2 = sr2.getGradientAreaGap();
                            double normalizedA1 = normalizedArea(a1, maxAreaGap);
                            double normalizedA2 = normalizedArea(a2, maxAreaGap);
                            // reverse comparison by the score to normalized area ratio
                            return Double.compare(sr1.getMatchingPixelsPct() / normalizedA1, sr2.getMatchingPixelsPct() / normalizedA2);
                        };
                    }
                    break;
                case USE_MATCHING_SLICES_ONLY:
                default:
                    srComparator = Comparator.comparing(ColorMIPSearchResultMetadata::getMatchingPixels);
                    break;
            }

            Results<List<ColorMIPSearchResultMetadata>> resultsWithSortedContent = new Results<>(resultsFileContent.results.stream()
                    .sorted(srComparator.reversed())
                    .collect(Collectors.toList()));
            if (StringUtils.isBlank(outputDir)) {
                mapper.writerWithDefaultPrettyPrinter().writeValue(System.out, resultsWithSortedContent);
            } else {
                File outputResultsFile = new File(outputDir, inputResultsFile.getName());
                LOG.info("Writing {}", outputResultsFile);
                mapper.writerWithDefaultPrettyPrinter().writeValue(outputResultsFile, resultsWithSortedContent);
            }
        } catch (IOException e) {
            LOG.error("Error reading {}", inputResultsFilename, e);
            throw new UncheckedIOException(e);
        }
    }

    private static double normalizedArea(long a, long maxArea) {
        if (a == -1) {
            return 0.004;
        } else {
            double r;
            if ((double) a / maxArea < 0.002) {
                r = 0.002;
            } else {
                r = (double) a / maxArea;
            }
            return r;
        }
    }

    private static void calculateGradientAreaScore(GradientScoreResultsArgs args) {
        LocalColorMIPSearch colorMIPSearch = new LocalColorMIPSearch(
                args.gradientPath,
                args.getOutputDir(),
                args.dataThreshold,
                args.maskThreshold,
                args.pixColorFluctuation,
                args.xyShift,
                args.negativeRadius,
                args.mirrorMask,
                args.pctPositivePixels,
                args.libraryPartitionSize,
                null);
        String outputDir = args.getOutputDir();
        if (args.resultsFile != null) {
            int from = Math.max(args.resultsFile.offset, 0);
            int length = args.resultsFile.length;
            calculateGradientAreaScoreForResultsFile(colorMIPSearch, args.resultsFile.input, from, length, outputDir);
        } else if (args.resultsDir != null) {
            try {
                int from = Math.max(args.resultsDir.offset, 0);
                int length = args.resultsDir.length;
                List<String> resultFileNames = Files.find(Paths.get(args.resultsDir.input), 1, (p, fa) -> fa.isRegularFile())
                        .skip(from)
                        .map(p -> p.toString())
                        .collect(Collectors.toList());
                if (length > 0 && length < resultFileNames.size()) {
                    resultFileNames.subList(0, length).parallelStream()
                            .forEach(p -> calculateGradientAreaScoreForResultsFile(colorMIPSearch, p, 0, -1, outputDir));
                } else {
                    resultFileNames.parallelStream()
                            .forEach(p -> calculateGradientAreaScoreForResultsFile(colorMIPSearch, p, 0, -1, outputDir));
                }
            } catch (IOException e) {
                LOG.error("Error listing {}", args.resultsDir, e);
            }
        }
    }

    private static void calculateGradientAreaScoreForResultsFile(LocalColorMIPSearch colorMIPSearch, String inputResultsFilename, int offset, int length, String outputDir) {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            LOG.info("Reading {}", inputResultsFilename);
            File inputResultsFile = new File(inputResultsFilename);
            Results<List<ColorMIPSearchResultMetadata>> resultsFileContent = mapper.readValue(inputResultsFile, new TypeReference<Results<List<ColorMIPSearchResultMetadata>>>() {});
            if (CollectionUtils.isEmpty(resultsFileContent.results)) {
                LOG.error("No color depth search results found in {}", inputResultsFile);
                return;
            }
            LOG.info("Finished reading {} entries from {}", resultsFileContent.results.size(), inputResultsFilename);
            long startTime = System.currentTimeMillis();
            Map<MIPInfo, List<ColorMIPSearchResultMetadata>> resultsGroupedById = resultsFileContent.results.stream()
                    .collect(Collectors.groupingBy(csr -> {
                        MIPInfo mip = new MIPInfo();
                        mip.id = csr.id;
                        mip.archivePath = csr.imageArchivePath;
                        mip.imagePath = csr.imageName;
                        mip.type = csr.imageType;
                        return mip;
                    }, Collectors.toList()));
            int from = Math.max(offset, 0);
            int to = length > 0 ? length : Integer.MAX_VALUE;
            Streams.zip(IntStream.range(0, to).boxed(), resultsGroupedById.entrySet().stream().skip(from), (i, resultsEntry) -> ImmutablePair.of(i + 1, resultsEntry))
                    .parallel()
                    .forEach(resultsEntry -> {
                        LOG.info("Calculate gradient area scores for matches of {} (entry# {}) from {}", resultsEntry.getRight().getKey(), resultsEntry.getLeft(), inputResultsFile);
                        long startTimeForCurrentEntry = System.currentTimeMillis();
                        MIPImage inputImage = colorMIPSearch.loadMIP(resultsEntry.getRight().getKey());
                        MIPImage inputGradientImage = colorMIPSearch.loadGradientMIP(resultsEntry.getRight().getKey());
                        resultsEntry.getRight().getValue().stream().parallel().forEach(csr ->{
                            MIPInfo matchedMIP = new MIPInfo();
                            matchedMIP.archivePath = csr.matchedImageArchivePath;
                            matchedMIP.imagePath = csr.matchedImageName;
                            matchedMIP.type = csr.matchedImageType;
                            MIPImage matchedImage = colorMIPSearch.loadMIP(matchedMIP);
                            MIPImage matchedGradientImage = colorMIPSearch.loadGradientMIP(matchedMIP);

                            ColorMIPSearchResult.AreaGap areaGap = colorMIPSearch.calculateGradientAreaAdjustment(inputImage, inputGradientImage, matchedImage, matchedGradientImage);

                            if (areaGap != null)
                                csr.setGradientAreaGap(areaGap.value); // update current result

                        });
                        LOG.info("Finished gradient area scores for matches of {} (entry# {}) from {} in {}s", resultsEntry.getRight().getKey(), resultsEntry.getLeft(), inputResultsFilename, (System.currentTimeMillis()-startTimeForCurrentEntry)/1000.);
                    });
            LOG.info("Finished gradient area score for all {} entries from {} in {}s", resultsFileContent.results.size(), inputResultsFilename, (System.currentTimeMillis()-startTime)/1000.);

            if (StringUtils.isBlank(outputDir)) {
                mapper.writerWithDefaultPrettyPrinter().writeValue(System.out, resultsFileContent);
            } else {
                File outputResultsFile = new File(outputDir, inputResultsFile.getName());
                LOG.info("Writing {}", outputResultsFile);
                mapper.writerWithDefaultPrettyPrinter().writeValue(outputResultsFile, resultsFileContent);
            }
        } catch (IOException e) {
            LOG.error("Error reading {}", inputResultsFilename, e);
            throw new UncheckedIOException(e);
        }
    }

}
