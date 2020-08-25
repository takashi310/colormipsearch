package org.janelia.colormipsearch.cmd;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

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
            File cdsResultsFile = new File(args.reverseResultsDir, mipId + DEFAULT_CDSRESULTS_EXT);
            LOG.debug("Read results from {}", cdsResultsFile);
            InputStream cdsResultsStream = null;
            try {
                cdsResultsStream = new FileInputStream(cdsResultsFile);
                return ColorMIPSearchResultUtils.readCDSMatchesFromJSONStream(cdsResultsStream, mapper)
                        .results.stream()
                        .filter(r -> r.getGradientAreaGap() != -1)
                        .collect(Collectors.toList());
            } catch (Exception e) {
                LOG.error("Error reading CDS results from {}", cdsResultsFile, e);
                return Collections.emptyList();
            } finally {
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
            Map<String, List<ColorMIPSearchMatchMetadata>> cdsMatchesMap = new LinkedHashMap<String, List<ColorMIPSearchMatchMetadata>>((int) cacheSize) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, List<ColorMIPSearchMatchMetadata>> eldest) {
                    LOG.debug("Remove {}", eldest.getKey());
                    return size() > cacheSize;
                }
            };
            cdsResultsLoader = (String mipId) -> {
                List<ColorMIPSearchMatchMetadata> matches = cdsMatchesMap.get(mipId);
                if (matches == null) {
                    matches = cdsResultsFileLoader.apply(mipId);
                    cdsMatchesMap.put(mipId, matches);
                }
                return matches;
            };
        } else {
            cdsResultsLoader = cdsResultsFileLoader;
        }
        CompletableFuture.allOf(Utils.partitionList(filesToProcess, args.processingPartitionSize).stream().parallel()
                .flatMap(fileList -> fileList.stream()
                        .map(File::new)
                        .map(f -> updateGradientScoresForFile(f, cdsResultsLoader, outputDir, executor))).toArray(CompletableFuture<?>[]::new)).join();
    }

    private CompletableFuture<CDSMatches> updateGradientScoresForFile(File f, Function<String, List<ColorMIPSearchMatchMetadata>> cdsResultsSupplier, Path outputDir, Executor executor) {
        CDSMatches cdsMatches = ColorMIPSearchResultUtils.readCDSMatchesFromJSONFile(f, mapper);
        if (CollectionUtils.isNotEmpty(cdsMatches.results)) {
            AtomicLong startTime = new AtomicLong(0L);
            return CompletableFuture.allOf(cdsMatches.results.stream()
                    .map(cdsr -> CompletableFuture.supplyAsync(() -> {
                        long startSingleResultUpdate = System.currentTimeMillis();
                        startTime.compareAndSet(0L, startSingleResultUpdate);
                        findReverserseResult(cdsr, cdsResultsSupplier)
                                .ifPresent(reverseCdsr -> {
                                    LOG.debug("Set gradient area gap for {} from {} to {} in {}ms",
                                            cdsr, reverseCdsr, reverseCdsr.getGradientAreaGap(), System.currentTimeMillis() - startSingleResultUpdate);
                                    cdsr.setGradientAreaGap(reverseCdsr.getGradientAreaGap());
                                    cdsr.setNormalizedGapScore(reverseCdsr.getNormalizedGapScore());
                                });
                        return cdsr;
                    }, executor)).toArray(CompletableFuture<?>[]::new))
                .thenApply(vr -> {
                    LOG.info("Finished updating {} results from {} in {}ms",
                            cdsMatches.results.size(), f, System.currentTimeMillis() - startTime.get());
                    ColorMIPSearchResultUtils.sortCDSResults(cdsMatches.results);
                    ColorMIPSearchResultUtils.writeCDSMatchesToJSONFile(
                            cdsMatches,
                            CmdUtils.getOutputFile(outputDir, f),
                            args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter());
                    return cdsMatches;
                });
        } else {
            return CompletableFuture.completedFuture(cdsMatches);
        }
    }

    private Optional<ColorMIPSearchMatchMetadata> findReverserseResult(ColorMIPSearchMatchMetadata cdsr, Function<String, List<ColorMIPSearchMatchMetadata>> cdsResultsSupplier) {
        List<ColorMIPSearchMatchMetadata> matches = cdsResultsSupplier.apply(cdsr.getId());
        return matches.stream()
                .filter(csr -> csr.matches(cdsr))
                .findFirst();
    }
}
