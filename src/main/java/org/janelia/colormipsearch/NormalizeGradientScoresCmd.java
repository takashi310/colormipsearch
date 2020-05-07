package org.janelia.colormipsearch;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
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

class NormalizeGradientScoresCmd {
    private static final Logger LOG = LoggerFactory.getLogger(NormalizeGradientScoresCmd.class);

    @Parameters(commandDescription = "Normalize gradient score for the search results - " +
            "if the area gap is not available consider it as if there was a perfect shape match, i.e., areagap = 0")
    static class NormalizeGradientScoresArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, variableArity = true, description = "Results directory to be normalized")
        List<String> resultsDirs;

        @Parameter(names = {"--resultsFile", "-rf"}, variableArity = true, description = "File containing results to be normalized")
        List<String> resultsFiles;

        @Parameter(names = {"--pctPositivePixels"}, description = "% of Positive PX Threshold (0-100%)")
        Double pctPositivePixels = 0.0;

        @Parameter(names = "-cleanup", description = "Cleanup results and remove fields not necessary in productiom", arity = 0)
        private boolean cleanup = false;

        @ParametersDelegate
        final CommonArgs commonArgs;

        NormalizeGradientScoresArgs(CommonArgs commonArgs) {
            this.commonArgs = commonArgs;
        }

        Path getOutputDir() {
            if (StringUtils.isNotBlank(commonArgs.outputDir)) {
                return Paths.get(commonArgs.outputDir);
            } else {
                return null;
            }
        }
    }

    private final NormalizeGradientScoresArgs args;

    NormalizeGradientScoresCmd(CommonArgs commonArgs) {
        args =  new NormalizeGradientScoresArgs(commonArgs);
    }

    NormalizeGradientScoresArgs getArgs() {
        return args;
    }

    void execute() {
        setGradientScores(args);
    }

    private static void setGradientScores(NormalizeGradientScoresArgs args) {
        List<String> resultFileNames;
        if (CollectionUtils.isNotEmpty(args.resultsFiles)) {
            resultFileNames = args.resultsFiles;
        } else if (CollectionUtils.isNotEmpty(args.resultsDirs)) {
            resultFileNames = args.resultsDirs.stream()
                    .flatMap(rd -> {
                        try {
                            return Files.find(Paths.get(rd), 1, (p, fa) -> fa.isRegularFile());
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .map(p -> p.toString())
                    .collect(Collectors.toList());
        } else {
            resultFileNames = Collections.emptyList();
        }
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        resultFileNames.stream().parallel().forEach((fn) -> {
            LOG.info("Set gradient score results for {}", fn);
            File cdsFile = new File(fn);
            Results<List<ColorMIPSearchResultMetadata>> cdsResults = CmdUtils.readCDSResultsFromJSONFile(cdsFile , mapper);
            Long maxAreaGap = cdsResults.results.stream()
                    .map(ColorMIPSearchResultMetadata::getGradientAreaGap)
                    .max(Long::compare)
                    .orElse(-1L);
            LOG.debug("Max area gap for {}  -> {}", fn, maxAreaGap);
            Integer maxMatchingPixels = cdsResults.results.stream()
                    .map(ColorMIPSearchResultMetadata::getMatchingPixels)
                    .max(Integer::compare)
                    .orElse(0);
            LOG.debug("Max pixel match for {}  -> {}", fn, maxMatchingPixels);
            List<ColorMIPSearchResultMetadata> cdsResultsWithNormalizedScore = cdsResults.results.stream()
                    .filter(csr -> csr.getMatchingPixelsPct() * 100. > args.pctPositivePixels)
                    .peek(csr -> {
                        long areaGap = csr.getGradientAreaGap();
                        if (areaGap >= 0) {
                            csr.setNormalizedGradientAreaGapScore(GradientAreaGapUtils.calculateAreaGapScore(
                                    csr.getGradientAreaGap(), maxAreaGap, csr.getMatchingPixels(), csr.getMatchingPixelsPct(), maxMatchingPixels)
                            );
                        } else {
                            csr.setArtificialShapeScore(GradientAreaGapUtils.calculateAreaGapScore(
                                    0, Math.max(maxAreaGap, 0), csr.getMatchingPixels(), csr.getMatchingPixelsPct(), maxMatchingPixels)
                            );
                        }
                    })
                    .collect(Collectors.toList());
            CmdUtils.sortCDSResults(cdsResultsWithNormalizedScore);
            CmdUtils.writeCDSResultsToJSONFile(new Results<>(cdsResultsWithNormalizedScore), CmdUtils.getOutputFile(args.getOutputDir(), new File(fn)), mapper);
        });
    }


}
