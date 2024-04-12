package org.janelia.colormipsearch.cmd_v2;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

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
import org.janelia.colormipsearch.api_v2.Utils;
import org.janelia.colormipsearch.api_v2.cdsearch.CDSMatches;
import org.janelia.colormipsearch.api_v2.cdsearch.ColorMIPSearchMatchMetadata;
import org.janelia.colormipsearch.api_v2.cdsearch.ColorMIPSearchResultUtils;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
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

    static class ReverseColorDepthMatchesProvider {
        private static LoadingCache<String, Map<String, List<LightColorMIPSearchMatch>>> cdsMatchesCache;
        private static Function<String, Map<String, List<LightColorMIPSearchMatch>>> cdsMatchesLoader;

        static void initializeCache(long maxSize, Function<String, Map<String, List<LightColorMIPSearchMatch>>> cdsMatchesLoader) {
            ReverseColorDepthMatchesProvider.cdsMatchesLoader = cdsMatchesLoader;
            if (maxSize > 0) {
                LOG.info("Initialize cache: size={}", maxSize);
                cdsMatchesCache = CacheBuilder.newBuilder()
                        .concurrencyLevel(8)
                        .maximumSize(maxSize)
                        .build(new CacheLoader<String, Map<String, List<LightColorMIPSearchMatch>>>() {
                            @Override
                            public Map<String, List<LightColorMIPSearchMatch>> load(@Nonnull String filepath) {
                                long startCacheLoad = System.currentTimeMillis();
                                try {
                                    return cdsMatchesLoader.apply(filepath);
                                } finally {
                                    LOG.debug("Loaded {} into cache in {}ms", filepath, (System.currentTimeMillis()-startCacheLoad));
                                }
                            }
                        });
            } else {
                cdsMatchesCache = null;
            }
        }

        private final String fp;

        ReverseColorDepthMatchesProvider(String fp) {
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

        Map<String, List<LightColorMIPSearchMatch>> getCdsMatches() {
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
        // initialize the cache for the reverse matches
        ReverseColorDepthMatchesProvider.initializeCache(
                cacheSizeSupplier.get(),
                fn -> loadLightCdsMatches(fn).stream()
                        .filter(LightColorMIPSearchMatch::hasNegativeScore)
                        .collect(Collectors.groupingBy(LightColorMIPSearchMatch::getId, Collectors.toList()))
        );
        updateGradientScores(args);
    }

    private void updateGradientScores(UpdateGradientScoresArgs args) {
        long startTime = System.currentTimeMillis();
        Executor executor = CmdUtils.createCDSExecutor(args.commonArgs);
        LOG.info("Prepare reverse results cache from {}", args.reverseResultsDir);
        Map<String, ReverseColorDepthMatchesProvider> reverseCDSResultsCache =
                CmdUtils.getFileToProcessFromDir(args.reverseResultsDir, 0, -1).stream().parallel()
                    .map(ReverseColorDepthMatchesProvider::new)
                    .collect(Collectors.toMap(ReverseColorDepthMatchesProvider::getMipId, Function.identity()));
        LOG.info("Done preparing reverse results cache from {} in {}ms", args.reverseResultsDir, System.currentTimeMillis() - startTime);

        List<String> filesToProcess;
        if (CollectionUtils.isNotEmpty(args.resultsFiles)) {
            filesToProcess = args.resultsFiles;
        } else if (args.resultsDir != null) {
            filesToProcess = CmdUtils.getFileToProcessFromDir(args.resultsDir.input, args.resultsDir.offset, args.resultsDir.length);
        } else {
            filesToProcess = Collections.emptyList();
        }
        int nFiles = filesToProcess.size();
        Path outputDir = args.getOutputDir();
        List<CompletableFuture<List<String>>> updateGradientComputations = ItemsHandling.partitionCollection(filesToProcess, args.processingPartitionSize).entrySet().stream().parallel()
                .map(indexedFilesPartition -> CompletableFuture.supplyAsync(() -> {
                    long startProcessingPartition = System.currentTimeMillis();
                    for (String fn : indexedFilesPartition.getValue()) {
                        updateGradientScoresForFile(
                                fn,
                                mipId -> {
                                    ReverseColorDepthMatchesProvider reverseCDSMatchesProvider = reverseCDSResultsCache.get(mipId);
                                    if (reverseCDSMatchesProvider == null) {
                                        return Collections.emptyMap();
                                    } else {
                                        return reverseCDSMatchesProvider.getCdsMatches();
                                    }
                                },
                                outputDir);
                    }
                    LOG.info("Processed {} files in {}s - memory usage {}M",
                            indexedFilesPartition.getValue().size(),
                            (System.currentTimeMillis() - startProcessingPartition) / 1000.,
                            (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1);
                    return indexedFilesPartition.getValue();
                }, executor))
                .collect(Collectors.toList())
        ;
        int nComputations = updateGradientComputations.size();
        LOG.info("Created {} computations to update gradient scores for {} files in {}s - memory used so far {}M",
                nComputations,
                nFiles,
                (System.currentTimeMillis() - startTime) / 1000.,
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1);
        CompletableFuture.allOf(updateGradientComputations.toArray(new CompletableFuture<?>[0]))
                .join();
        LOG.info("Completed {} computations to update gradient scores for {} files in {}s - memory usage {}M",
                nComputations,
                nFiles,
                (System.currentTimeMillis() - startTime) / 1000.,
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1);
    }

    private void updateGradientScoresForFile(String filepath,
                                             Function<String, Map<String, List<LightColorMIPSearchMatch>>> reverseCDSMatchesProvider,
                                             Path outputDir) {
        CDSMatches cdsMatches = loadCdsMatches(filepath);
        if (cdsMatches.isEmpty()) {
            return; // either something went wrong or there's really nothing to do
        }
        long startTime = System.currentTimeMillis();
        LOG.info("Start processing {} containing {} results", filepath, cdsMatches.getResults().size());
        int nUpdates = cdsMatches.getResults().stream()
                .mapToInt(cdsr -> findReverseMatches(cdsr, reverseCDSMatchesProvider.apply(cdsr.getId()))
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
                nUpdates, cdsMatches.getResults().size(), filepath, System.currentTimeMillis() - startTime);
        try {
            ColorMIPSearchResultUtils.sortCDSResults(cdsMatches.getResults());
            ColorMIPSearchResultUtils.writeCDSMatchesToJSONFile(
                    cdsMatches,
                    CmdUtils.getOutputFile(outputDir, new File(filepath)),
                    args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter());
        } catch (Exception e) {
            LOG.error("Failed to update {} with gradient scores", filepath, e);
            throw new IllegalStateException(e);
        }
    }

    private List<LightColorMIPSearchMatch> loadLightCdsMatches(String filepath) {
        long startLoadTime = System.currentTimeMillis();
        try {
            LOG.info("Read {}", filepath);
            LightColorMIPSearchResults cdsMatches = mapper.readValue(new File(filepath), LightColorMIPSearchResults.class);
            return cdsMatches == null || cdsMatches.isEmpty() ? Collections.emptyList() : cdsMatches.getResults();
        } catch (Exception e) {
            LOG.error("Error reading {}", filepath, e);
            throw new IllegalStateException(e);
        } finally {
            LOG.debug("Loaded {} in {}ms", filepath, (System.currentTimeMillis()-startLoadTime));
        }

    }

    private CDSMatches loadCdsMatches(String filepath) {
        long startLoadTime = System.currentTimeMillis();
        try {
            CDSMatches cdsMatches = ColorMIPSearchResultUtils.readCDSMatchesFromJSONFilePath(Paths.get(filepath), mapper);
            return cdsMatches == null || cdsMatches.isEmpty() ? CDSMatches.EMPTY : cdsMatches;
        } catch (Exception e) {
            LOG.error("Error reading {}", filepath, e);
            throw new IllegalStateException(e);
        } finally {
            LOG.debug("Loaded {} in {}ms", filepath, (System.currentTimeMillis()-startLoadTime));
        }
    }

    private Optional<LightColorMIPSearchMatch> findReverseMatches(ColorMIPSearchMatchMetadata cdsr, Map<String, List<LightColorMIPSearchMatch>> cdsReverseMatchesMap) {
        List<LightColorMIPSearchMatch> cdsReverseMatches = cdsReverseMatchesMap.get(cdsr.getSourceId());
        if (cdsReverseMatches == null) {
            return Optional.empty();
        } else {
            return cdsReverseMatches.stream()
                    .filter(r -> r.exactMatch(cdsr))
                    .findFirst()
                    .map(m -> Optional.of(m))
                    .orElseGet(() -> cdsReverseMatches.stream().filter(r -> r.mipMatch(cdsr)).findFirst());
        }
    }
}
