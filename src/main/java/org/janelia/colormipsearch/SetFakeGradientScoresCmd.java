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

class SetFakeGradientScoresCmd {
    private static final Logger LOG = LoggerFactory.getLogger(SetFakeGradientScoresCmd.class);

    @Parameters(commandDescription = "Combine color depth search results")
    static class SetGradientScoresArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, variableArity = true, description = "Results directory to be sorted")
        List<String> resultsDirs;

        @Parameter(names = {"--resultsFile", "-rf"}, variableArity = true, description = "File containing results to be sorted")
        List<String> resultsFiles;

        @Parameter(names = {"--pctPositivePixels"}, description = "% of Positive PX Threshold (0-100%)")
        Double pctPositivePixels = 0.0;

        @Parameter(names = "-cleanup", description = "Cleanup results and remove fields not necessary in productiom", arity = 0)
        private boolean cleanup = false;

        @ParametersDelegate
        final CommonArgs commonArgs;

        SetGradientScoresArgs(CommonArgs commonArgs) {
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

    private final SetGradientScoresArgs args;

    SetFakeGradientScoresCmd(CommonArgs commonArgs) {
        args =  new SetGradientScoresArgs(commonArgs);
    }

    SetGradientScoresArgs getArgs() {
        return args;
    }

    void execute() {
        setGradientScores(args);
    }

    private static void setGradientScores(SetGradientScoresArgs args) {
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
        EM2LMAreaGapCalculator emlmAreaGapCalculator = new EM2LMAreaGapCalculator(0, 0, false);

        resultFileNames.stream().parallel().forEach((fn) -> {
            LOG.info("Set gradient score results for {}", fn);
            File cdsFile = new File(fn);
            Results<List<ColorMIPSearchResultMetadata>> cdsResults = CmdUtils.readCDSResultsFromJSONFile(cdsFile , mapper);
            Double maxPctPixelScore = cdsResults.results.stream()
                    .map(ColorMIPSearchResultMetadata::getMatchingPixelsPct)
                    .max(Double::compare)
                    .orElse(0.);
            LOG.debug("Max pixel percentage score for {}  -> {}", fn, maxPctPixelScore);
            List<ColorMIPSearchResultMetadata> cdsResultsWithNormalizedScore = cdsResults.results.stream()
                    .filter(csr -> csr.getMatchingPixelsPct() * 100. > args.pctPositivePixels)
                    .peek(csr -> {
                        csr.setArtificialShapeScore(emlmAreaGapCalculator.calculateAreaGapScore(
                                0, 0, csr.getMatchingPixelsPct(), maxPctPixelScore)
                        );
                    })
                    .collect(Collectors.toList());
            CmdUtils.sortCDSResults(cdsResultsWithNormalizedScore);
            CmdUtils.writeCDSResultsToJSONFile(new Results<>(cdsResultsWithNormalizedScore), CmdUtils.getOutputFile(args.getOutputDir(), new File(fn)), mapper);
        });
    }


}
