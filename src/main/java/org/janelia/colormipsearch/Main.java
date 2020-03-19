package org.janelia.colormipsearch;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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

        @Parameter(names = {"--gradientDir", "-gd"}, description = "Gradient masks directory")
        String gradientDir;

        @Parameter(names = "-cdsConcurrency", description = "CDS concurrency - number of CDS tasks run concurrently")
        int cdsConcurrency = 100;

        @ParametersDelegate
        final CommonArgs commonArgs;

        AbstractArgs(CommonArgs commonArgs) {
            this.commonArgs = commonArgs;
        }

        String getOutputDir() {
            return commonArgs.outputDir;
        }
    }

    @Parameters(commandDescription = "Single color depth search")
    private static class SingleSearchArgs extends AbstractArgs {
        @Parameter(names = "-i", required = true, description = "Library MIP path")
        private String libraryPath;

        @Parameter(names = "-m", required = true, description = "Mask path")
        private String maskPath;

        @Parameter(names = "-result", description = "Result file name")
        private String resultName;

        SingleSearchArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }
    }

    @Parameters(commandDescription = "Batch color depth search")
    private static class BatchSearchArgs extends AbstractArgs {
        @Parameter(names = {"--images", "-i"}, required = true, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "Comma-delimited list of directories containing images to search")
        private List<ListArg> librariesInputs;

        @Parameter(names = {"--masks", "-m"}, required = true, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "Image file(s) to use as the search masks")
        private List<ListArg> masksInputs;

        @Parameter(names = "-locally", description = "Perform the search in the current process", arity = 0)
        private boolean useLocalProcessing = false;

        BatchSearchArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }

        boolean useSpark() {
            return !useLocalProcessing;
        }
    }

    @Parameters(commandDescription = "Sort color depth search results")
    private static class SortResultsArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, description = "Results directory to be sorted")
        private String resultsDir;

        @Parameter(names = {"--resultsFile", "-rf"}, description = "File containing results to be sorted")
        private String resultsFile;

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
        @Parameter(names = {"--resultsDir", "-rd"}, description = "Results directory to be sorted")
        private String resultsDir;

        @Parameter(names = {"--resultsFile", "-rf"}, description = "File containing results to be sorted")
        private String resultsFile;

        GradientScoreResultsArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }

        String getOutputDir() {
            return StringUtils.defaultIfBlank(commonArgs.outputDir, resultsDir);
        }
    }

    public static void main(String[] argv) {
        MainArgs mainArgs = new MainArgs();
        CommonArgs commonArgs = new CommonArgs();
        BatchSearchArgs batchSearchArgs = new BatchSearchArgs(commonArgs);
        SingleSearchArgs singleSearchArgs = new SingleSearchArgs(commonArgs);
        SortResultsArgs sortResultsArgs = new SortResultsArgs(commonArgs);
        GradientScoreResultsArgs gradientScoreResultsArgs = new GradientScoreResultsArgs(commonArgs);

        JCommander cmdline = JCommander.newBuilder()
                .addObject(mainArgs)
                .addCommand("batch", batchSearchArgs)
                .addCommand("singleSearch", singleSearchArgs)
                .addCommand("sortResults", sortResultsArgs)
                .addCommand("gradientScore", gradientScoreResultsArgs)
                .build();

        try {
            cmdline.parse(argv);
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder(e.getMessage()).append('\n');
            cmdline.usage(sb);
            JCommander.getConsole().println(sb.toString());
            System.exit(1);
        }

        if (mainArgs.displayHelpMessage || commonArgs.displayHelpMessage) {
            cmdline.usage();
            System.exit(0);
        }

        if (StringUtils.isBlank(cmdline.getParsedCommand())) {
            StringBuilder sb = new StringBuilder("Missing command\n");
            cmdline.usage(sb);
            JCommander.getConsole().println(sb.toString());
            System.exit(1);
        }
        switch (cmdline.getParsedCommand()) {
            case "batch":
                createOutputDir(batchSearchArgs.getOutputDir());
                runBatchSearch(batchSearchArgs);
                break;
            case "singleSearch":
                createOutputDir(singleSearchArgs.getOutputDir());
                runSingleSearch(singleSearchArgs);
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
            case "gradientScore":
                if (StringUtils.isBlank(gradientScoreResultsArgs.resultsDir) && StringUtils.isBlank(gradientScoreResultsArgs.resultsFile)) {
                    StringBuilder sb = new StringBuilder("No result file or directory containing results has been specified").append('\n');
                    cmdline.usage(sb);
                    System.exit(1);
                }
                createOutputDir(gradientScoreResultsArgs.getOutputDir());
                calculateGradientAreaScore(gradientScoreResultsArgs);
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

    private static void runBatchSearch(BatchSearchArgs args) {
        ColorMIPSearch colorMIPSearch;
        if (args.useSpark()) {
            colorMIPSearch = new SparkColorMIPSearch(
                    args.appName, args.gradientDir, args.getOutputDir(), args.dataThreshold, args.maskThreshold, args.pixColorFluctuation, args.xyShift, args.negativeRadius, args.mirrorMask, args.pctPositivePixels
            );
        } else {
            colorMIPSearch = new LocalColorMIPSearch(
                    args.gradientDir,
                    args.getOutputDir(), args.dataThreshold, args.maskThreshold, args.pixColorFluctuation, args.xyShift, args.negativeRadius, args.mirrorMask, args.pctPositivePixels, args.cdsConcurrency);
        }

        try {
            List<MIPInfo> libraryMips = args.librariesInputs.stream()
                    .flatMap(libraryInput -> readMIPs(libraryInput).stream())
                    .collect(Collectors.toList());

            List<MIPInfo> masksMips = args.masksInputs.stream()
                    .flatMap(masksInput -> readMIPs(masksInput).stream())
                    .collect(Collectors.toList());

            colorMIPSearch.compareEveryMaskWithEveryLibrary(masksMips, libraryMips);
        } finally {
            colorMIPSearch.terminate();
        }
    }

    private static void runSingleSearch(SingleSearchArgs args) {
        LocalColorMIPSearch colorMIPSearch = new LocalColorMIPSearch(args.gradientDir, args.getOutputDir(), args.dataThreshold, args.maskThreshold, args.pixColorFluctuation, args.xyShift, args.negativeRadius, args.mirrorMask, args.pctPositivePixels, args.cdsConcurrency);
        try {
            MIPImage libraryMIP = colorMIPSearch.loadMIPFromPath(Paths.get(args.libraryPath));
            MIPImage libraryGradient = colorMIPSearch.loadGradientMIP(libraryMIP.mipInfo);
            MIPImage patternMIP = colorMIPSearch.loadMIPFromPath(Paths.get(args.maskPath));
            MIPImage patternGradient = colorMIPSearch.loadGradientMIP(patternMIP.mipInfo);
            ColorMIPSearchResult searchResult = colorMIPSearch.applyGradientAreaAdjustment(colorMIPSearch.runImageComparison(libraryMIP, patternMIP), libraryMIP, libraryGradient, patternMIP, patternGradient);
            LOG.info("Search result: {}", searchResult);
            colorMIPSearch.writeSearchResults(args.resultName, Collections.singletonList(searchResult.perLibraryMetadata()));
        } finally {
            colorMIPSearch.terminate();
        }
    }

    private static void sortResults(SortResultsArgs args) {
        String outputDir = args.getOutputDir();
        if (StringUtils.isNotBlank(args.resultsFile)) {
            sortResultsFile(args.resultsFile, outputDir);
        } else if (StringUtils.isNotBlank(args.resultsDir)) {
            try {
                Files.find(Paths.get(args.resultsDir), 1, (p, fa) -> fa.isRegularFile())
                        .forEach(p -> sortResultsFile(p.toString(), outputDir));
            } catch (IOException e) {
                LOG.error("Error listing {}", args.resultsDir, e);
            }
        }
    }

    private static void sortResultsFile(String inputResultsFilename, String outputDir) {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            LOG.info("Reading {}", inputResultsFilename);
            File inputResultsFile = new File(inputResultsFilename);
            Results<List<ColorMIPSearchResultMetadata>> resultsFileContent = mapper.readValue(inputResultsFile, new TypeReference<Results<List<ColorMIPSearchResultMetadata>>>() {
            });
            long maxAreaGap = resultsFileContent.results.stream()
                    .mapToLong(ColorMIPSearchResultMetadata::getGradientAreaGap)
                    .filter(a -> a != -1)
                    .max()
                    .orElse(-1);
            Results<List<ColorMIPSearchResultMetadata>> resultsWithSortedContent = new Results<>(resultsFileContent.results.stream()
                    .sorted((sr1, sr2) -> {
                        // this is completely empirical because I don't know
                        // how to compare the results that have no area gap with the ones that have
                        if (maxAreaGap == -1) {
                            return Comparator.comparing(ColorMIPSearchResultMetadata::getMatchingSlices).reversed().compare(sr1, sr2);
                        } else {
                            long a1 = sr1.getGradientAreaGap() ;
                            long a2 = sr2.getGradientAreaGap();
                            double normalizedA1 = normalizedArea(a1, maxAreaGap);
                            double normalizedA2 = normalizedArea(a2, maxAreaGap);
                            // reverse comparison by the score to normalized area ratio
                            return Double.compare(sr2.getMatchingSlicesPct() / normalizedA2, sr1.getMatchingSlicesPct() / normalizedA1);
                        }
                    })
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
        LocalColorMIPSearch colorMIPSearch = new LocalColorMIPSearch(args.gradientDir, args.getOutputDir(), args.dataThreshold, args.maskThreshold, args.pixColorFluctuation, args.xyShift, args.negativeRadius, args.mirrorMask, args.pctPositivePixels, args.cdsConcurrency);
        String outputDir = args.getOutputDir();
        if (StringUtils.isNotBlank(args.resultsFile)) {
            calculateGradientAreaScoreForResultsFile(colorMIPSearch, args.resultsFile, outputDir);
        } else if (StringUtils.isNotBlank(args.resultsDir)) {
            try {
                Files.find(Paths.get(args.resultsDir), 1, (p, fa) -> fa.isRegularFile())
                        .forEach(p -> calculateGradientAreaScoreForResultsFile(colorMIPSearch, p.toString(), outputDir));
            } catch (IOException e) {
                LOG.error("Error listing {}", args.resultsDir, e);
            }
        }
    }

    private static void calculateGradientAreaScoreForResultsFile(LocalColorMIPSearch colorMIPSearch, String inputResultsFilename, String outputDir) {
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
            MIPImage inputMIP = colorMIPSearch.loadMIPFromPath(Paths.get(resultsFileContent.results.get(0).imageName));
            MIPImage inputGradientMIP = colorMIPSearch.loadGradientMIP(inputMIP.mipInfo);
            Results<List<ColorMIPSearchResultMetadata>> resultsWithAreaGradientAdjustment = new Results<>(resultsFileContent.results.stream()
                    .peek(csr -> {
                        MIPImage matchedMIP = colorMIPSearch.loadMIPFromPath(Paths.get(csr.matchedImageName));
                        MIPImage matchedGradientMIP = colorMIPSearch.loadGradientMIP(matchedMIP.mipInfo);
                        ColorMIPSearchResult.AreaGap areaGap = colorMIPSearch.calculateGradientAreaAdjustment(inputMIP, inputGradientMIP, matchedMIP, matchedGradientMIP);
                        if (areaGap != null)
                            csr.setGradientAreaGap(areaGap.value);
                    })
                    .collect(Collectors.toList()));
            if (StringUtils.isBlank(outputDir)) {
                mapper.writerWithDefaultPrettyPrinter().writeValue(System.out, resultsWithAreaGradientAdjustment);
            } else {
                File outputResultsFile = new File(outputDir, inputResultsFile.getName());
                LOG.info("Writing {}", outputResultsFile);
                mapper.writerWithDefaultPrettyPrinter().writeValue(outputResultsFile, resultsWithAreaGradientAdjustment);
            }
        } catch (IOException e) {
            LOG.error("Error reading {}", inputResultsFilename, e);
            throw new UncheckedIOException(e);
        }

    }

    private static List<MIPInfo> readMIPs(ListArg mipsArg) {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            LOG.info("Reading {}", mipsArg);
            List<MIPInfo> content = mapper.readValue(new File(mipsArg.input), new TypeReference<List<MIPInfo>>() {
            });
            int from = mipsArg.offset > 0 ? mipsArg.offset : 0;
            int to = mipsArg.length > 0 ? Math.min(from + mipsArg.length, content.size()) : content.size();
            LOG.info("Read {} mips from {} starting at {} to {}", content.size(), mipsArg, from, to);
            return content.subList(from, to);
        } catch (IOException e) {
            LOG.error("Error reading {}", mipsArg, e);
            throw new UncheckedIOException(e);
        }
    }

}
