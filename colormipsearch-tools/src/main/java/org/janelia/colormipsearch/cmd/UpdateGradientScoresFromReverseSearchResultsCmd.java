package org.janelia.colormipsearch.cmd;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
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
import com.google.common.cache.LoadingCache;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.api.Utils;
import org.janelia.colormipsearch.api.cdsearch.CDSMatches;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchMatchMetadata;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResultUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UpdateGradientScoresFromReverseSearchResultsCmd extends AbstractCmd {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateGradientScoresFromReverseSearchResultsCmd.class);

    @Parameters(commandDescription = "Update gradient area score from the reverse search results, " +
            "e.g set gradient score for LM to EM search results from EM to LM results or vice-versa")
    static class UpdateGradientScoresArgs extends AbstractCmdArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, converter = ListArg.ListArgConverter.class,
                description = "Results directory for which the gradients need to be set")
        private ListArg resultsDir;

        @Parameter(names = {"--resultsFile", "-rf"}, variableArity = true, description = "File(s) containing results to be calculated")
        private List<String> resultsFiles;

        @Parameter(names = {"--reverseResultsDir", "-revd"},
                description = "Directory containing matches that we want to be updated with the corresponding gradient scores from <resultsDir>")
        private String reverseResultsDir;

        @Parameter(names = {"--processingPartitionSize", "-ps"}, description = "Processing partition size")
        int processingPartitionSize = 10;

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

    static class ColorDepthSearchMatchesProvider {
        private static LoadingCache<String, List<ColorMIPSearchMatchMetadata>> cdsMatchesCache;
        private static Function<String, List<ColorMIPSearchMatchMetadata>> cdsMatchesLoader;

        static void initializeCache(long maxSize, long expirationInSeconds, Function<String, List<ColorMIPSearchMatchMetadata>> cdsMatchesLoader) {
            ColorDepthSearchMatchesProvider.cdsMatchesLoader = cdsMatchesLoader;
            if (maxSize > 0) {
                LOG.info("Initialize cache: size={} and expiration={}s", maxSize, expirationInSeconds);
                CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder()
                        .concurrencyLevel(8)
                        .maximumSize(maxSize);
                if (expirationInSeconds > 0) {
                    cacheBuilder.expireAfterAccess(Duration.ofSeconds(expirationInSeconds));
                }
                cdsMatchesCache = cacheBuilder
                        .build(new CacheLoader<String, List<ColorMIPSearchMatchMetadata>>() {
                            @Override
                            public List<ColorMIPSearchMatchMetadata> load(String filepath) {
                                return cdsMatchesLoader.apply(filepath);
                            }
                        });
            } else {
                cdsMatchesCache = null;
            }
        }

        private final String fp;

        ColorDepthSearchMatchesProvider(String fp) {
            this.fp = fp;
        }

        String getMipId() {
            String fn = Paths.get(fp).getFileName().toString();
            int extseparator = fn.lastIndexOf('.');
            if (extseparator != -1) {
                return fn.substring(0, extseparator);
            } else {
                return fn;
            }
        }

        List<ColorMIPSearchMatchMetadata> getCdsMatches() {
            if (cdsMatchesCache == null) {
                return cdsMatchesLoader.apply(fp);
            } else {
                try {
                    return cdsMatchesCache.get(fp);
                } catch (ExecutionException e) {
                    LOG.error("Error accessing {} from cache", fp, e);
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    private final UpdateGradientScoresArgs args;
    private final Supplier<Long> cacheSizeSupplier;
    private final Supplier<Long> cacheExpirationInSecondsSupplier;
    private final ObjectMapper mapper;

    UpdateGradientScoresFromReverseSearchResultsCmd(String commandName,
                                                    CommonArgs commonArgs,
                                                    Supplier<Long> cacheSizeSupplier,
                                                    Supplier<Long> cacheExpirationInSecondsSupplier) {
        super(commandName);
        this.args = new UpdateGradientScoresArgs(commonArgs);
        this.cacheSizeSupplier = cacheSizeSupplier;
        this.cacheExpirationInSecondsSupplier = cacheExpirationInSecondsSupplier;
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
        // initialize the cache for the reverse matches
        ColorDepthSearchMatchesProvider.initializeCache(
                cacheSizeSupplier.get(),
                cacheExpirationInSecondsSupplier.get(),
                fn -> loadCdsMatches(fn).getResults().stream().parallel()
                        .filter(cdsr -> cdsr.getNegativeScore() != -1)
                        .collect(Collectors.toList())
        );
        updateGradientScores(args);
    }

    private void updateGradientScores(UpdateGradientScoresArgs args) {
        long startTime = System.currentTimeMillis();
        LOG.info("Prepare opposite results cache from {}", args.reverseResultsDir);
        Map<String, ColorDepthSearchMatchesProvider> reverseCDSResultsCache =
                CmdUtils.getFileToProcessFromDir(args.reverseResultsDir, 0, -1).stream().parallel()
                    .map(ColorDepthSearchMatchesProvider::new)
                    .collect(Collectors.toMap(ColorDepthSearchMatchesProvider::getMipId, cdsMatches -> cdsMatches));
        LOG.info("Done preparing reverse results cache from {} in {}ms", args.reverseResultsDir, System.currentTimeMillis() - startTime);

        List<String> filesToProcess;
        if (CollectionUtils.isNotEmpty(args.resultsFiles)) {
            filesToProcess = args.resultsFiles;
        } else if (args.resultsDir != null) {
            filesToProcess = CmdUtils.getFileToProcessFromDir(args.resultsDir.input, args.resultsDir.offset, args.resultsDir.length);
        } else {
            filesToProcess = Collections.emptyList();
        }
        Path outputDir = args.getOutputDir();
        Utils.partitionCollection(filesToProcess, args.processingPartitionSize).stream().parallel()
                .forEach(fileList -> {
                    long startProcessingPartition = System.currentTimeMillis();
                    for (String f : fileList) {
                        updateGradientScoresForFile(f,
                                mipId -> {
                                    ColorDepthSearchMatchesProvider reverseCDSMatchesProvider = reverseCDSResultsCache.get(mipId);
                                    if (reverseCDSMatchesProvider == null) {
                                        return Collections.emptyList();
                                    } else {
                                        return reverseCDSMatchesProvider.getCdsMatches();
                                    }
                                },
                                outputDir);
                    }
                    LOG.info("Processed {} files in {}s - memory usage {}M",
                            fileList.size(),
                            (System.currentTimeMillis() - startProcessingPartition) / 1000.,
                            (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1);
                });
        LOG.info("Completed updating gradient scores for {} files in {}s - memory usage {}M",
                filesToProcess.size(),
                (System.currentTimeMillis() - startTime) / 1000.,
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1);
    }

    private void updateGradientScoresForFile(String filepath,
                                             Function<String, List<ColorMIPSearchMatchMetadata>> cdsResultsMapProvider,
                                             Path outputDir) {
        CDSMatches cdsMatches = loadCdsMatches(filepath);
        if (CollectionUtils.isEmpty(cdsMatches.results)) {
            return; // either something went wrong or there's really nothing to do
        }
        long startTime = System.currentTimeMillis();
        LOG.info("Start processing {} for updating gradient scores", filepath);
        int nUpdates = cdsMatches.results.stream().parallel()
                .mapToInt(cdsr -> findReverseMatches(cdsr, cdsResultsMapProvider.apply(cdsr.getId()))
                        .map(reverseCdsr -> {
                            LOG.debug("Set negative scores for {} from {} to {}, {}",
                                    cdsr, reverseCdsr, reverseCdsr.getGradientAreaGap(), reverseCdsr.getHighExpressionArea());
                            cdsr.setGradientAreaGap(reverseCdsr.getGradientAreaGap());
                            cdsr.setHighExpressionArea(reverseCdsr.getHighExpressionArea());
                            cdsr.setNormalizedGapScore(reverseCdsr.getNormalizedGapScore());
                            return 1;
                        })
                        .orElse(0))
                .sum();
        LOG.info("Finished updating {} results out of {} from {} in {}ms",
                nUpdates, cdsMatches.results.size(), filepath, System.currentTimeMillis() - startTime);
        try {
            ColorMIPSearchResultUtils.sortCDSResults(cdsMatches.results);
            ColorMIPSearchResultUtils.writeCDSMatchesToJSONFile(
                    cdsMatches,
                    CmdUtils.getOutputFile(outputDir, new File(filepath)),
                    args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter());
        } catch (Exception e) {
            LOG.error("Failed to update {} with gradient scores", filepath, e);
            throw new IllegalStateException(e);
        }
    }

    private CDSMatches loadCdsMatches(String filepath) {
        try {
            CDSMatches cdsMatches = ColorMIPSearchResultUtils.readCDSMatchesFromJSONFilePath(Paths.get(filepath), mapper);
            return cdsMatches == null || cdsMatches.isEmpty() ? CDSMatches.EMPTY : cdsMatches;
        } catch (Exception e) {
            LOG.error("Error reading {}", filepath, e);
            throw new IllegalStateException(e);
        }
    }

    private Optional<ColorMIPSearchMatchMetadata> findReverseMatches(ColorMIPSearchMatchMetadata cdsr, List<ColorMIPSearchMatchMetadata> cdsReverseMatches) {
        return cdsReverseMatches.stream().parallel()
                .filter(r -> r.matches(cdsr))
                .findFirst();
    }
}
