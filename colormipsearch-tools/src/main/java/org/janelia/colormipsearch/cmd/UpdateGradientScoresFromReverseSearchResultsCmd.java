package org.janelia.colormipsearch.cmd;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        private final String fp;
        private CDSMatches cdsMatches;

        ColorDepthSearchMatchesProvider(String fp) {
            this.fp = fp;
            this.cdsMatches = null;
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

        String getCdsFilename() {
            return fp;
        }

        File getCdsFile() {
            return new File(fp);
        }

        synchronized boolean hasCdsMatches() {
            return cdsMatches != null;
        }

        synchronized CDSMatches getCdsMatches(ObjectMapper mapper, Predicate<ColorMIPSearchMatchMetadata> filter) {
            if (cdsMatches == null) {
                try {
                    cdsMatches = ColorMIPSearchResultUtils.readCDSMatchesFromJSONFilePath(Paths.get(fp), mapper, filter);
                } catch (IOException e) {
                    LOG.error("Error reading {}", fp, e);
                    throw new UncheckedIOException(e);
                }
            }
            return cdsMatches;
        }
    }

    private final UpdateGradientScoresArgs args;
    private final ObjectMapper mapper;

    UpdateGradientScoresFromReverseSearchResultsCmd(String commandName, CommonArgs commonArgs) {
        super(commandName);
        this.args = new UpdateGradientScoresArgs(commonArgs);
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
        long startTime = System.currentTimeMillis();
        LOG.info("Prepare opposite results cache from {}", args.reverseResultsDir);
        Map<String, ColorDepthSearchMatchesProvider> oppositeResultsCache = streamCDSMatchesFromFiles(getFileToProcessFromDir(args.reverseResultsDir, 0, -1))
                .collect(Collectors.toMap(cdsMatches -> cdsMatches.getMipId(), cdsMatches -> cdsMatches));
        LOG.info("Done preparing opposite results cache from {} in {}ms", args.reverseResultsDir, System.currentTimeMillis() - startTime);

        List<String> filesToProcess;
        if (CollectionUtils.isNotEmpty(args.resultsFiles)) {
            filesToProcess = args.resultsFiles;
        } else if (args.resultsDir != null) {
            filesToProcess = getFileToProcessFromDir(args.resultsDir.input, args.resultsDir.offset, args.resultsDir.length);
        } else {
            filesToProcess = Collections.emptyList();
        }
        Path outputDir = args.getOutputDir();
        Utils.partitionList(filesToProcess, args.processingPartitionSize).stream().parallel()
                .forEach(fileList -> streamCDSMatchesFromFiles(fileList)
                        .forEach(cdsMatches -> updateGradientScoresForCDSMatches(cdsMatches,
                                mipId -> {
                                    CDSMatches oppositeCDSMatches;
                                    ColorDepthSearchMatchesProvider oppositeCDSMatchesProvider = oppositeResultsCache.get(mipId);
                                    if (oppositeCDSMatchesProvider == null) {
                                        oppositeCDSMatches = null;
                                    } else {
                                        oppositeCDSMatches = oppositeCDSMatchesProvider.getCdsMatches(mapper, cdsr -> cdsr.getNegativeScore() != -1);
                                    }
                                    if (oppositeCDSMatches == null) {
                                        return Collections.emptyList();
                                    } else {
                                        return oppositeCDSMatches.results;
                                    }
                                },
                                outputDir)));
        long loadedOppositeMatches = oppositeResultsCache.entrySet().stream().parallel()
                .filter(e -> e.getValue().hasCdsMatches())
                .count();
        LOG.info("Completed updating gradient scores for {} files with {} opposite matches loaded out of {} in {}s - memory usage {}M",
                filesToProcess.size(),
                loadedOppositeMatches,
                oppositeResultsCache.size(),
                (System.currentTimeMillis() - startTime) / 1000.,
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1);
    }

    private List<String> getFileToProcessFromDir(String dirName, int offsetParam, int lengthParam) {
        try {
            int from = Math.max(offsetParam, 0);
            List<String> filenamesList = Files.find(Paths.get(dirName), 1, (p, fa) -> fa.isRegularFile())
                    .skip(from)
                    .map(Path::toString)
                    .collect(Collectors.toList());
            if (lengthParam > 0 && lengthParam < filenamesList.size()) {
                return filenamesList.subList(0, lengthParam);
            } else {
                return filenamesList;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Stream<ColorDepthSearchMatchesProvider> streamCDSMatchesFromFiles(List<String> fileList) {
        return fileList.stream()
                .map(ColorDepthSearchMatchesProvider::new)
                ;
    }

    private void updateGradientScoresForCDSMatches(ColorDepthSearchMatchesProvider cdsMatchesProvider,
                                                   Function<String, List<ColorMIPSearchMatchMetadata>> cdsResultsSupplier,
                                                   Path outputDir) {
        CDSMatches cdsMatches = cdsMatchesProvider.getCdsMatches(mapper, null);
        if (cdsMatches == null || CollectionUtils.isEmpty(cdsMatches.results)) {
            return; // either something went wrong or there's really nothing to do
        }
        long startTime = System.currentTimeMillis();
        LOG.info("Start processing {} for updating gradient scores", cdsMatchesProvider.getCdsFilename());
        int nUpdates = cdsMatches.results.stream().parallel()
                .mapToInt(cdsr -> findReverserseResult(cdsr, cdsResultsSupplier)
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
                nUpdates, cdsMatches.results.size(), cdsMatchesProvider.getCdsFilename(), System.currentTimeMillis() - startTime);
        ColorMIPSearchResultUtils.sortCDSResults(cdsMatches.results);
        ColorMIPSearchResultUtils.writeCDSMatchesToJSONFile(
                cdsMatches,
                CmdUtils.getOutputFile(outputDir, cdsMatchesProvider.getCdsFile()),
                args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter());
    }

    private Optional<ColorMIPSearchMatchMetadata> findReverserseResult(ColorMIPSearchMatchMetadata cdsr, Function<String, List<ColorMIPSearchMatchMetadata>> cdsResultsSupplier) {
        List<ColorMIPSearchMatchMetadata> allMatchesForMatchedId = cdsResultsSupplier.apply(cdsr.getId());
        return allMatchesForMatchedId.stream()
                .filter(r -> r.getNegativeScore() != -1)
                .filter(r -> r.matches(cdsr))
                .findFirst();
    }
}
