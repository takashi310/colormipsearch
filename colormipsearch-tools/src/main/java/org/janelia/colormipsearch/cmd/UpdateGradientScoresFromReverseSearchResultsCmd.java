package org.janelia.colormipsearch.cmd;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.RemovalListener;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.colormipsearch.api.Utils;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api.cdsearch.CDSMatches;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchMatchMetadata;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResultUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UpdateGradientScoresFromReverseSearchResultsCmd extends AbstractCmd {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateGradientScoresFromReverseSearchResultsCmd.class);
    private static final String DEFAULT_CDSRESULTS_EXT = ".json";

    @Parameters(commandDescription = "Update gradient area score from the reverse search results, " +
            "e.g set gradient score for LM to EM search results from EM to LM results or vice-versa")
    static class UpdateGradientScoresArgs extends AbstractCmdArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, converter = ListArg.ListArgConverter.class,
                description = "Results directory for which the gradients need to be set")
        private ListArg resultsDir;

        @Parameter(names = {"--resultsFile", "-rf"}, variableArity = true, description = "File containing results to be calculated")
        private List<String> resultsFiles;

        @Parameter(names = {"--reverseResultsDir", "-revd"}, description = "Reverse results directory to be calculated")
        private String reverseResultsDir;

        @Parameter(names = {"--processingPartitionSize", "-ps"}, description = "Processing partition size")
        int processingPartitionSize = 100;

        @Parameter(names = {"--topPublishedNameMatches"},
                description = "If set only calculate the gradient score for the specified number of best lines color depth search results")
        int numberOfBestLines;

        @Parameter(names = {"--topPublishedSampleMatches"},
                description = "If set select the specified numnber of best samples for each line to calculate the gradient score")
        int numberOfBestSamplesPerLine;

        @Parameter(names = {"--topMatchesPerSample"},
                description = "Number of best matches for each line to be used for gradient scoring (defaults to 1)")
        int numberOfBestMatchesPerSample;

        @ParametersDelegate
        final CommonArgs commonArgs;

        UpdateGradientScoresArgs(CommonArgs commonArgs) {
            this.commonArgs = commonArgs;
        }

        Path getOutputDir() {
            if (StringUtils.isNotBlank(commonArgs.outputDir)) {
                return Paths.get(commonArgs.outputDir);
            } else {
                return null;
            }
        }

        @Override
        List<String> validate() {
            List<String> errors = new ArrayList<>();
            boolean inputFound = resultsDir != null || CollectionUtils.isNotEmpty(resultsFiles);
            if (!inputFound) {
                errors.add("No result file or directory containing results has been specified");
            }
            return errors;
        }
    }

    private final UpdateGradientScoresArgs args;
    private final Supplier<Long> cacheSizeSupplier;
    private final ObjectMapper mapper;

    UpdateGradientScoresFromReverseSearchResultsCmd(String commandName,
                                                    CommonArgs commonArgs,
                                                    Supplier<Long> cacheSizeSupplier) {
        super(commandName);
        this.args = new UpdateGradientScoresArgs(commonArgs);
        this.cacheSizeSupplier = cacheSizeSupplier;
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    UpdateGradientScoresArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        CmdUtils.createOutputDirs(args.getOutputDir());
        Executor executor = CmdUtils.createCDSExecutor(args.commonArgs);
        updateGradientScores(args, executor);
    }

    private void updateGradientScores(UpdateGradientScoresArgs args, Executor executor) {
        List<String> filesToProcess;
        if (CollectionUtils.isNotEmpty(args.resultsFiles)) {
            filesToProcess = args.resultsFiles;
        } else if (args.resultsDir != null) {
            try {
                int from = Math.max(args.resultsDir.offset, 0);
                int length = args.resultsDir.length;
                List<String> filenamesList = Files.find(Paths.get(args.resultsDir.input), 1, (p, fa) -> fa.isRegularFile())
                        .skip(from)
                        .map(Path::toString)
                        .collect(Collectors.toList());
                if (length > 0 && length < filenamesList.size()) {
                    filesToProcess = filenamesList.subList(0, length);
                } else {
                    filesToProcess = filenamesList;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            filesToProcess = Collections.emptyList();
        }
        Path outputDir = args.getOutputDir();
        LOG.info("Prepare results loader for {}", args.reverseResultsDir);
        Function<String, List<ColorMIPSearchMatchMetadata>> cdsResultsFileLoader = (String mipId) -> {
            long startTime = System.currentTimeMillis();
            File cdsResultsFile = new File(args.reverseResultsDir, mipId + DEFAULT_CDSRESULTS_EXT);
            LOG.debug("Read results from {}", cdsResultsFile);
            InputStream cdsResultsStream = null;
            try {
                cdsResultsStream = new BufferedInputStream(new FileInputStream(cdsResultsFile));
                return ColorMIPSearchResultUtils.readCDSMatchesFromJSONStream(cdsResultsStream, mapper)
                        .results.stream()
                        .filter(r -> r.getGradientAreaGap() != -1)
                        .collect(Collectors.toList());
            } catch (Exception e) {
                LOG.error("Error reading CDS results from {}", cdsResultsFile, e);
                return Collections.emptyList();
            } finally {
                LOG.debug("Finished reading results from {} in {}ms", cdsResultsFile, System.currentTimeMillis()-startTime);
                if (cdsResultsStream != null) {
                    try {
                        cdsResultsStream.close();
                    } catch (IOException ignore) {
                    }
                }
            }
        };
        long cacheSize = cacheSizeSupplier.get();
        Function<String, List<ColorMIPSearchMatchMetadata>> cdsResultsLoader;
        if (cacheSize > 0) {
            cdsResultsLoader = CacheBuilder.newBuilder()
                    .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                    .maximumSize(cacheSize)
                    .removalListener((RemovalListener<String, List<ColorMIPSearchMatchMetadata>>) notification -> {
                        if (notification.wasEvicted()) {
                            LOG.info("Evicted {}", notification.getKey());
                        }
                    })
                    .build(new CacheLoader<String, List<ColorMIPSearchMatchMetadata>>() {
                        @Override
                        public List<ColorMIPSearchMatchMetadata> load(String mipId) {
                            return cdsResultsFileLoader.apply(mipId);
                        }
                    });
        } else {
            cdsResultsLoader = cdsResultsFileLoader;
        }
        Utils.partitionList(filesToProcess, args.processingPartitionSize).stream().parallel()
                .forEach(fileList -> fileList.stream()
                        .map(File::new)
                        .forEach(f -> updateGradientScoresForResultsFile(f,
                                cdsResultsLoader,
                                args.numberOfBestLines,
                                args.numberOfBestSamplesPerLine,
                                args.numberOfBestMatchesPerSample,
                                outputDir))
                );
    }

    private void updateGradientScoresForResultsFile(File cdsMatchesFile,
                                                    Function<String, List<ColorMIPSearchMatchMetadata>> cdsResultsSupplier,
                                                    int numberOfBestLinesToSelect,
                                                    int numberOfBestSamplesPerLineToSelect,
                                                    int numberOfBestMatchesPerSampleToSelect,
                                                    Path outputDir) {
        CDSMatches cdsMatchesContent = ColorMIPSearchResultUtils.readCDSMatchesFromJSONFile(cdsMatchesFile, mapper);
        if (CollectionUtils.isEmpty(cdsMatchesContent.results)) {
            LOG.error("No color depth search results found in {}", cdsMatchesFile);
            return;
        }
        Map<MIPMetadata, List<ColorMIPSearchMatchMetadata>> resultsGroupedById = ColorMIPSearchResultUtils.selectCDSResultForGradientScoreCalculation(
                cdsMatchesContent.results,
                numberOfBestLinesToSelect,
                numberOfBestSamplesPerLineToSelect,
                numberOfBestMatchesPerSampleToSelect);
        LOG.info("Read {} entries ({} distinct mask MIPs) from {}", cdsMatchesContent.results.size(), resultsGroupedById.size(), cdsMatchesFile);
        long startTime = System.currentTimeMillis();
        Map<String, List<ColorMIPSearchMatchMetadata>> reverseCDSMatches = resultsGroupedById.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream())
                .map(cdsr -> cdsr.getId())
                .collect(Collectors.collectingAndThen(
                        Collectors.toSet(),
                        matchedMIPsIds -> {
                            LOG.info("Read {} reverse matches for {}", matchedMIPsIds.size(), cdsMatchesFile);
                            return matchedMIPsIds.stream().parallel()
                                    .map(mipId -> ImmutablePair.of(mipId, cdsResultsSupplier.apply(mipId)))
                                    .collect(Collectors.toMap(ImmutablePair::getLeft, ImmutablePair::getRight));
                        }));
        LOG.info("Read {} reverse matches out of {} for {} in {}ms",
                reverseCDSMatches.size(),
                cdsMatchesContent.results.size(),
                cdsMatchesFile,
                System.currentTimeMillis()-startTime);
        int nUpdates = cdsMatchesContent.results.stream()
                .mapToInt(cdsr -> findReverserseResult(cdsr, reverseCDSMatches::get)
                        .map(reverseCdsr -> {
                            LOG.debug("Set gradient area gap for {} from {} to {}",
                                    cdsr, reverseCdsr, reverseCdsr.getGradientAreaGap());
                            cdsr.setGradientAreaGap(reverseCdsr.getGradientAreaGap());
                            cdsr.setNormalizedGapScore(reverseCdsr.getNormalizedGapScore());
                            return 1;
                        })
                        .orElse(0))
                .sum();
        LOG.info("Finished updating {} results out of {} from {} in {}ms",
                nUpdates, cdsMatchesContent.results.size(), cdsMatchesFile, System.currentTimeMillis()-startTime);
        ColorMIPSearchResultUtils.sortCDSResults(cdsMatchesContent.results);
        ColorMIPSearchResultUtils.writeCDSMatchesToJSONFile(
                cdsMatchesContent,
                CmdUtils.getOutputFile(outputDir, cdsMatchesFile),
                args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter());
    }

    private Optional<ColorMIPSearchMatchMetadata> findReverserseResult(ColorMIPSearchMatchMetadata cdsr, Function<String, List<ColorMIPSearchMatchMetadata>> cdsResultsSupplier) {
        List<ColorMIPSearchMatchMetadata> matches = cdsResultsSupplier.apply(cdsr.getId());
        return matches.stream()
                .filter(csr -> csr.matches(cdsr))
                .findFirst();
    }
}
