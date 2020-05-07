package org.janelia.colormipsearch;

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
import java.util.stream.Collectors;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UpdateGradientScoresFromReverseSearchResultsCmd {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateGradientScoresFromReverseSearchResultsCmd.class);

    @Parameters(commandDescription = "Update gradient area score from the reverse search results, " +
            "e.g set gradient score for LM to EM search results from EM to LM results or vice-versa")
    static class GradientScoreResultsArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, description = "Results directory to be calculated")
        private String resultsDir;

        @Parameter(names = {"--resultsFile", "-rf"}, variableArity = true, description = "File containing results to be calculated")
        private List<String> resultsFiles;

        @Parameter(names = {"--reverseResultsDir", "-revd"}, description = "Reverse results directory to be calculated")
        private String reverseResultsDir;

        @ParametersDelegate
        final CommonArgs commonArgs;

        GradientScoreResultsArgs(CommonArgs commonArgs) {
            this.commonArgs = commonArgs;
        }

        String getResultsDir() {
            return resultsDir;
        }

        List<String> getResultsFiles() {
            return resultsFiles;
        }

        boolean validate() {
            return StringUtils.isNotBlank(resultsDir) || CollectionUtils.isNotEmpty(resultsFiles);
        }

        Path getOutputDir() {
            if (StringUtils.isNotBlank(commonArgs.outputDir)) {
                return Paths.get(commonArgs.outputDir);
            } else {
                return null;
            }
        }
    }

    private final GradientScoreResultsArgs args;

    UpdateGradientScoresFromReverseSearchResultsCmd(CommonArgs commonArgs) {
        this.args = new GradientScoreResultsArgs(commonArgs);
    }

    GradientScoreResultsArgs getArgs() {
        return args;
    }

    void execute() {
        updateGradientAreaScores(args);
    }

    private void updateGradientAreaScores(GradientScoreResultsArgs args) {
        List<String> resultFileNames;
        if (CollectionUtils.isNotEmpty(args.resultsFiles)) {
            resultFileNames = args.resultsFiles;
        } else if (StringUtils.isNotEmpty(args.resultsDir)) {
            try {
                resultFileNames = Files.find(Paths.get(args.resultsDir), 1, (p, fa) -> fa.isRegularFile())
                        .map(p -> p.toString())
                        .collect(Collectors.toList());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            resultFileNames = Collections.emptyList();
        }
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        Path outputDir = args.getOutputDir();
        resultFileNames.stream().parallel()
                .map(File::new)
                .forEach(f -> {
                    Results<List<ColorMIPSearchResultMetadata>> cdsResults = CmdUtils.readCDSResultsFromJSONFile(f, mapper);
                    if (CollectionUtils.isNotEmpty(cdsResults.results)) {
                        Set<String> matchedIds = cdsResults.results.stream().map(csr -> csr.matchedId).collect(Collectors.toSet());
                        LOG.info("Reading reverse results from {}:{}", args.reverseResultsDir, matchedIds);
                        Map<String, List<ColorMIPSearchResultMetadata>> reverseResults = readMatchIdResults(args.reverseResultsDir, matchedIds, mapper);
                        cdsResults.results
                                .forEach(csr -> {
                                    ColorMIPSearchResultMetadata reverseCsr = findReverserseResult(csr, reverseResults);
                                    if (reverseCsr == null) {
                                        LOG.warn("No matching result found for {}", csr);
                                    } else {
                                        csr.setGradientAreaGap(reverseCsr.getGradientAreaGap());
                                        csr.setNormalizedGradientAreaGapScore(reverseCsr.getNormalizedGradientAreaGapScore());
                                    }
                                });
                        CmdUtils.sortCDSResults(cdsResults.results);
                        CmdUtils.writeCDSResultsToJSONFile(cdsResults, CmdUtils.getOutputFile(outputDir, f), mapper);
                    }
                });
    }

    private Map<String, List<ColorMIPSearchResultMetadata>> readMatchIdResults(String resultsDir, Set<String> matchIds, ObjectMapper mapper) {
        return matchIds.stream()
                .map(matchId -> new File(resultsDir, matchId + ".json"))
                .filter(f -> f.exists())
                .map(fn -> CmdUtils.readCDSResultsFromJSONFile(fn, mapper))
                .filter(cdsResults -> cdsResults.results != null)
                .flatMap(cdsResults -> cdsResults.results.stream())
                .collect(Collectors.groupingBy(csr -> csr.matchedId, Collectors.toList()));
    }

    private ColorMIPSearchResultMetadata findReverserseResult(ColorMIPSearchResultMetadata result, Map<String, List<ColorMIPSearchResultMetadata>> indexedResults) {
        List<ColorMIPSearchResultMetadata> matches = indexedResults.get(result.id);
        if (matches == null) {
            return null;
        } else {
            return matches.stream()
                    .filter(csr -> csr.matchedId.equals(result.id))
                    .filter(csr -> csr.id.equals(result.matchedId))
                    .findFirst()
                    .orElse(null);
        }
    }
}
