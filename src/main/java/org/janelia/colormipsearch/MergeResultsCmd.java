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

class MergeResultsCmd {
    private static final Logger LOG = LoggerFactory.getLogger(MergeResultsCmd.class);

    @Parameters(commandDescription = "Merge color depth search results")
    static class MergeResultsArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, variableArity = true, description = "Results directory to be combined")
        List<String> resultsDirs;

        @Parameter(names = {"--resultsFile", "-rf"}, variableArity = true, description = "File containing results to be combined")
        List<String> resultsFiles;

        @Parameter(names = {"--pctPositivePixels"}, description = "% of Positive PX Threshold (0-100%)")
        Double pctPositivePixels = 0.0;

        @Parameter(names = "-cleanup", description = "Cleanup results and remove fields not necessary in productiom", arity = 0)
        private boolean cleanup = false;

        @ParametersDelegate
        final CommonArgs commonArgs;

        MergeResultsArgs(CommonArgs commonArgs) {
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

    private final MergeResultsArgs args;

    MergeResultsCmd(CommonArgs commonArgs) {
        args =  new MergeResultsArgs(commonArgs);
    }

    MergeResultsArgs getArgs() {
        return args;
    }

    void execute() {
        combineResults(args);
    }

    private void combineResults(MergeResultsArgs args) {
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
        combineResultFiles(resultFileNames, args.pctPositivePixels, args.cleanup, args.getOutputDir());
    }

    private void combineResultFiles(List<String> inputResultsFilenames, double pctPositivePixels, boolean cleanup, Path outputDir) {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // files that have the same file name (but coming from different directories)
        // will be combined in a single result file.
        Map<String, List<String>> resultFilesToCombinedTogether = inputResultsFilenames.stream()
                .collect(Collectors.groupingBy(fn -> Paths.get(fn).getFileName().toString(), Collectors.toList()));

        resultFilesToCombinedTogether.entrySet().stream().parallel()
                .forEach(e -> {
                    String fn = e.getKey();
                    List<String> resultList = e.getValue();
                    LOG.info("Combine results for {}", fn);
                    List<ColorMIPSearchResultMetadata> combinedResults = resultList.stream()
                            .map(cdsFn -> new File(cdsFn))
                            .map(cdsFile -> {
                                LOG.info("Reading {} -> {}", fn, cdsFile);
                                Results<List<ColorMIPSearchResultMetadata>> cdsResults = ColorMIPSearchResultUtils.readCDSResultsFromJSONFile(cdsFile, mapper);
                                if (cdsResults.results == null) {
                                    LOG.warn("Results file {} is empty", cdsFile);
                                }
                                return cdsResults;
                            })
                            .filter(cdsResults -> CollectionUtils.isNotEmpty(cdsResults.results))
                            .flatMap(cdsResults -> cdsResults.results.stream())
                            .filter(cdsr -> cdsr.getMatchingPixelsPct() * 100 > pctPositivePixels)
                            .map(cdsr -> cleanup ? ColorMIPSearchResultMetadata.create(cdsr) : cdsr)
                            .collect(Collectors.toList());
                    List<ColorMIPSearchResultMetadata> combinedResultsWithNoDuplicates = Utils.pickBestMatches(
                            combinedResults,
                            csr -> csr.matchedId,
                            csr -> (double) csr.getMatchingPixels(),
                            -1,
                            1)
                            .stream()
                            .flatMap(se -> se.entry.stream()).collect(Collectors.toList());

                    ColorMIPSearchResultUtils.sortCDSResults(combinedResultsWithNoDuplicates);
                    ColorMIPSearchResultUtils.writeCDSResultsToJSONFile(new Results<>(combinedResultsWithNoDuplicates), CmdUtils.getOutputFile(outputDir, new File(fn)), mapper);
                });
    }

}
