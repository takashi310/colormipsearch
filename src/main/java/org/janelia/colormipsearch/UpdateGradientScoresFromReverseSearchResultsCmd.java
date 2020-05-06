package org.janelia.colormipsearch;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Streams;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.colormipsearch.imageprocessing.TriFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UpdateGradientScoresFromReverseSearchResultsCmd {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateGradientScoresFromReverseSearchResultsCmd.class);

    @Parameters(commandDescription = "Update gradient area score from the reverse search results, " +
            "e.g set gradient score for LM to EM search results from EM to LM results or vice-versa")
    static class GradientScoreResultsArgs extends AbstractArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, description = "Results directory to be calculated")
        private String resultsDir;

        @Parameter(names = {"--resultsFile", "-rf"}, converter = ListArg.ListArgConverter.class, description = "File containing results to be calculated")
        private String resultsFile;

        @Parameter(names = {"--reverseResultsDir", "-revd"}, description = "Reverse results directory to be calculated")
        private String reverseResultsDir;

        GradientScoreResultsArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }

        String getResultsDir() {
            return resultsDir;
        }

        String getResultsFile() {
            return resultsFile;
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
        calculateGradientAreaScore(args);
    }

    private void calculateGradientAreaScore(GradientScoreResultsArgs args) {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        Path outputDir = args.getOutputDir();
    }



}
