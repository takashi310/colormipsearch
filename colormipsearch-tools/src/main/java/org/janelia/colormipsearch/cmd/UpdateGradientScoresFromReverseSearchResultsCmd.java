package org.janelia.colormipsearch.cmd;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.ParametersAreNonnullByDefault;

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
import org.janelia.colormipsearch.api.cdmips.AbstractMetadata;
import org.janelia.colormipsearch.api.cdsearch.CDSMatches;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchMatchMetadata;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResultUtils;
import org.janelia.colormipsearch.api.Utils;
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
        updateGradientScores(args);
    }

    private void updateGradientScores(UpdateGradientScoresArgs args) {
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
        LOG.info("Prepare cache loader for {} with cache size={} and cache expiration={}s",
                args.reverseResultsDir, cacheSizeSupplier.get(), cacheExpirationInSecondsSupplier.get());
        LoadingCache<String, List<ColorMIPSearchMatchMetadata>> reverseResultsCache = CacheBuilder.newBuilder()
                .concurrencyLevel(16)
                .expireAfterAccess(Duration.ofSeconds(cacheExpirationInSecondsSupplier.get()))
                .maximumSize(cacheSizeSupplier.get())
                .build(new CacheLoader<String, List<ColorMIPSearchMatchMetadata>>() {
                    @Override
                    public List<ColorMIPSearchMatchMetadata> load(String cdsMatchesId) {
                        File cdsMatchesFile = new File(args.reverseResultsDir, cdsMatchesId + DEFAULT_CDSRESULTS_EXT);
                        if (!cdsMatchesFile.exists()) {
                            return Collections.emptyList();
                        } else {
                            LOG.debug("Read reverse results from {}", cdsMatchesFile);
                            return ColorMIPSearchResultUtils.readCDSMatchesFromJSONFile(cdsMatchesFile, mapper)
                                    .results.stream()
                                    .filter(r -> r.getGradientAreaGap() != -1)
                                    .collect(Collectors.toList());
                        }
                    }
                });

        Utils.partitionList(filesToProcess, args.processingPartitionSize).stream().parallel()
                .forEach(fileList -> {
                    long startProcessingPartitionTime = System.currentTimeMillis();
                    fileList.stream()
                            .map(File::new)
                            .forEach(f -> {
                                long startTime = System.currentTimeMillis();
                                CDSMatches cdsMatches = ColorMIPSearchResultUtils.readCDSMatchesFromJSONFile(f, mapper);
                                if (CollectionUtils.isNotEmpty(cdsMatches.results)) {
                                    Set<String> sourceIds = cdsMatches.results.stream().map(ColorMIPSearchMatchMetadata::getSourceId).collect(Collectors.toSet());
                                    Set<String> matchedIds = cdsMatches.results.stream().map(AbstractMetadata::getId).collect(Collectors.toSet());
                                    LOG.debug("Reading {} reverse results for {} from {}", matchedIds.size(), f, args.reverseResultsDir);
                                    Map<String, List<ColorMIPSearchMatchMetadata>> reverseResults = readMatchIdResults(sourceIds, matchedIds, reverseResultsCache);
                                    LOG.info("Finished reading {} reverse results for {} from {} in {}ms",
                                            matchedIds.size(), f, args.reverseResultsDir, System.currentTimeMillis() - startTime);
                                    cdsMatches.results.stream().parallel()
                                            .forEach(csr -> {
                                                ColorMIPSearchMatchMetadata reverseCsr = findReverserseResult(csr, reverseResults);
                                                if (reverseCsr == null) {
                                                    LOG.debug("No matching result found for {}", csr);
                                                } else {
                                                    LOG.debug("Set gradient area gap for {} from {} to {}", csr, reverseCsr, reverseCsr.getGradientAreaGap());
                                                    csr.setGradientAreaGap(reverseCsr.getGradientAreaGap());
                                                    csr.setNormalizedGapScore(reverseCsr.getNormalizedGapScore());
                                                }
                                            });
                                    LOG.info("Finished updating {} results from {} in {}ms",
                                            cdsMatches.results.size(), f, System.currentTimeMillis() - startTime);
                                    ColorMIPSearchResultUtils.sortCDSResults(cdsMatches.results);
                                    ColorMIPSearchResultUtils.writeCDSMatchesToJSONFile(
                                            cdsMatches,
                                            CmdUtils.getOutputFile(outputDir, f),
                                            args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter());
                                }
                            });
                    LOG.info("Finished a batch of {} in {}s", fileList.size(), (System.currentTimeMillis() - startProcessingPartitionTime) / 1000.);
                });
    }

    private Map<String, List<ColorMIPSearchMatchMetadata>> readMatchIdResults(Set<String> ids, Set<String> matchIds, LoadingCache<String, List<ColorMIPSearchMatchMetadata>> reverseResultsCache) {
        return matchIds.stream().parallel()
                .flatMap(matchId -> {
                    try {
                        return streamMatchResultsWithGradientScore(reverseResultsCache.get(matchId), ids);
                    } catch (ExecutionException e) {
                        LOG.error("Error retrieving results for {}", matchId);
                        throw new IllegalStateException(e);
                    }
                })
                .collect(Collectors.groupingBy(AbstractMetadata::getId, Collectors.toList()));
    }

    private Stream<ColorMIPSearchMatchMetadata> streamMatchResultsWithGradientScore(List<ColorMIPSearchMatchMetadata> content, Set<String> ids) {
        if (content == null) {
            return Stream.of();
        } else {
            return content.stream()
                    .filter(n -> ids.contains(n.getId()))
                    .filter(n -> n.getGradientAreaGap() != -1)
                    ;
        }
    }

    private ColorMIPSearchMatchMetadata findReverserseResult(ColorMIPSearchMatchMetadata result, Map<String, List<ColorMIPSearchMatchMetadata>> indexedResults) {
        List<ColorMIPSearchMatchMetadata> matches = indexedResults.get(result.getSourceId());
        if (matches == null) {
            return null;
        } else {
            return matches.stream()
                    .filter(csr -> csr.matches(result))
                    .findFirst()
                    .orElse(null);
        }
    }
}
