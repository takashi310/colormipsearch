package org.janelia.colormipsearch.cmd;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.tools.ColorMIPSearchResultMetadata;
import org.janelia.colormipsearch.tools.ColorMIPSearchResultUtils;
import org.janelia.colormipsearch.tools.Results;
import org.janelia.colormipsearch.tools.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UpdateGradientScoresFromReverseSearchResultsCmd {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateGradientScoresFromReverseSearchResultsCmd.class);

    @Parameters(commandDescription = "Update gradient area score from the reverse search results, " +
            "e.g set gradient score for LM to EM search results from EM to LM results or vice-versa")
    static class GradientScoreResultsArgs {
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

        GradientScoreResultsArgs(CommonArgs commonArgs) {
            this.commonArgs = commonArgs;
        }

        Path getOutputDir() {
            if (StringUtils.isNotBlank(commonArgs.outputDir)) {
                return Paths.get(commonArgs.outputDir);
            } else {
                return null;
            }
        }

        boolean validate() {
            return resultsDir != null || CollectionUtils.isNotEmpty(resultsFiles);
        }
    }

    private final GradientScoreResultsArgs args;
    private final ObjectMapper mapper;

    UpdateGradientScoresFromReverseSearchResultsCmd(CommonArgs commonArgs) {
        this.args = new GradientScoreResultsArgs(commonArgs);
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    GradientScoreResultsArgs getArgs() {
        return args;
    }

    void execute() {
        updateGradientScores(args);
    }

    private void updateGradientScores(GradientScoreResultsArgs args) {
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
        Map<String, List<ColorMIPSearchResultMetadata>> reverseResultsCache = new ConcurrentHashMap<>();
        try {
            LOG.info("Read reverse results directory {}", args.reverseResultsDir);
            Files.find(Paths.get(args.reverseResultsDir), 1, (p, fa) -> fa.isRegularFile()).parallel()
                    .forEach(p -> {
                        String fn = p.getFileName().toString();
                        int extseparator = fn.lastIndexOf('.');
                        String id;
                        if (extseparator != -1) {
                            id = fn.substring(0, extseparator);
                        } else {
                            id = fn;
                        }
                        reverseResultsCache.put(
                                id,
                                ColorMIPSearchResultUtils.readCDSResultsFromJSONFile(
                                        p.toFile(), mapper).results.stream().filter(r -> r.getGradientAreaGap() != -1).collect(Collectors.toList())
                        );
                    });
            LOG.info("Finished reading reverse {} results from {}", reverseResultsCache.size(), args.reverseResultsDir);
        } catch (IOException e) {
            LOG.error("Error reading {}", args.reverseResultsDir);
            throw new UncheckedIOException(e);
        }

        Utils.partitionList(filesToProcess, args.processingPartitionSize).stream().parallel()
                .forEach(fileList -> {
                    long startProcessingPartitionTime = System.currentTimeMillis();
                    fileList.stream()
                            .map(File::new)
                            .forEach(f -> {
                                long startTime = System.currentTimeMillis();
                                Results<List<ColorMIPSearchResultMetadata>> cdsResults = ColorMIPSearchResultUtils.readCDSResultsFromJSONFile(f, mapper);
                                if (CollectionUtils.isNotEmpty(cdsResults.results)) {
                                    Set<String> ids = cdsResults.results.stream().map(csr -> csr.getId()).collect(Collectors.toSet());
                                    Set<String> matchedIds = cdsResults.results.stream().map(csr -> csr.getMatchedId()).collect(Collectors.toSet());
                                    LOG.info("Reading {} reverse results for {} from {}", matchedIds.size(), f, args.reverseResultsDir);
                                    Map<String, List<ColorMIPSearchResultMetadata>> reverseResults = readMatchIdResults(ids, matchedIds, reverseResultsCache);
                                    LOG.info("Finished reading {} reverse results for {} from {} in {}ms",
                                            matchedIds.size(), f, args.reverseResultsDir, System.currentTimeMillis() - startTime);
                                    cdsResults.results.stream()
                                            .forEach(csr -> {
                                                ColorMIPSearchResultMetadata reverseCsr = findReverserseResult(csr, reverseResults);
                                                if (reverseCsr == null) {
                                                    LOG.debug("No matching result found for {}", csr);
                                                } else {
                                                    csr.setGradientAreaGap(reverseCsr.getGradientAreaGap());
                                                    csr.setNormalizedGradientAreaGapScore(reverseCsr.getNormalizedGradientAreaGapScore());
                                                }
                                            });
                                    LOG.info("Finished updating {} results from {} in {}ms",
                                            cdsResults.results.size(), f, System.currentTimeMillis() - startTime);
                                    ColorMIPSearchResultUtils.sortCDSResults(cdsResults.results);
                                    ColorMIPSearchResultUtils.writeCDSResultsToJSONFile(
                                            cdsResults,
                                            CmdUtils.getOutputFile(outputDir, f),
                                            args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter());
                                }
                            });
                    LOG.info("Finished a batch of {} in {}s", fileList.size(), (System.currentTimeMillis() - startProcessingPartitionTime) / 1000.);
                });
    }

    private Map<String, List<ColorMIPSearchResultMetadata>> readMatchIdResults(Set<String> ids, Set<String> matchIds, Map<String, List<ColorMIPSearchResultMetadata>> reverseResults) {
        return matchIds.stream().parallel()
                .flatMap(matchId -> streamMatchResultsWithGradientScore(reverseResults.get(matchId), ids))
                .collect(Collectors.groupingBy(csr -> csr.getMatchedId(), Collectors.toList()));
    }

    private Stream<ColorMIPSearchResultMetadata> streamMatchResultsWithGradientScore(List<ColorMIPSearchResultMetadata> content, Set<String> ids) {
        if (content == null) {
            return Stream.of();
        } else {
            return content.stream()
                    .filter(n -> ids.contains(n.getMatchedId()))
                    .filter(n -> n.getGradientAreaGap() != -1)
                    ;
        }
    }

    private ColorMIPSearchResultMetadata findReverserseResult(ColorMIPSearchResultMetadata result, Map<String, List<ColorMIPSearchResultMetadata>> indexedResults) {
        List<ColorMIPSearchResultMetadata> matches = indexedResults.get(result.getId());
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
