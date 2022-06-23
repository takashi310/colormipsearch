package org.janelia.colormipsearch.cmd_v2;

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
import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.api_v2.cdsearch.CDSMatches;
import org.janelia.colormipsearch.api_v2.cdsearch.ColorMIPSearchMatchMetadata;
import org.janelia.colormipsearch.api_v2.cdsearch.ColorMIPSearchResultUtils;
import org.janelia.colormipsearch.api_v2.Utils;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MergeResultsCmd extends AbstractCmd {
    private static final Logger LOG = LoggerFactory.getLogger(MergeResultsCmd.class);

    @Parameters(commandDescription = "Merge color depth search results")
    static class MergeResultsArgs extends AbstractCmdArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, variableArity = true, description = "Results directory to be combined")
        List<String> resultsDirs;

        @Parameter(names = {"--resultsFile", "-rf"}, variableArity = true, description = "File containing results to be combined")
        List<String> resultsFiles;

        @Parameter(names = {"--pctPositivePixels"}, description = "% of Positive PX Threshold (0-100%)")
        Double pctPositivePixels = 0.0;

        @Parameter(names = "-cleanup", description = "Cleanup results and remove fields not necessary in productiom", arity = 0)
        boolean cleanup = false;

        @Parameter(names = {"--excluded-names"}, description = "Published names excluded from JSON", variableArity = true)
        List<String> excludedNames;

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

    MergeResultsCmd(String commandName, CommonArgs commonArgs) {
        super(commandName);
        args =  new MergeResultsArgs(commonArgs);
    }

    @Override
    MergeResultsArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        CmdUtils.createOutputDirs(args.getOutputDir());
        mergeResults(args);
    }

    private void mergeResults(MergeResultsArgs args) {
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
        mergeResultFiles(resultFileNames,
                args.pctPositivePixels,
                args.cleanup,
                CollectionUtils.isEmpty(args.excludedNames)
                        ? Collections.emptySet()
                        : ImmutableSet.copyOf(args.excludedNames),
                args.getOutputDir());
    }

    private void mergeResultFiles(List<String> inputResultsFilenames,
                                  double pctPositivePixels,
                                  boolean cleanup,
                                  Set<String> excludedNames,
                                  Path outputDir) {
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
                    List<ColorMIPSearchMatchMetadata> combinedResults = resultList.stream()
                            .map(File::new)
                            .map(cdsFile -> {
                                LOG.info("Reading {} -> {}", fn, cdsFile);
                                CDSMatches cdsResults = ColorMIPSearchResultUtils.readCDSMatchesFromJSONFile(cdsFile, mapper);
                                if (cdsResults.results == null) {
                                    LOG.warn("Results file {} is empty", cdsFile);
                                }
                                return cdsResults;
                            })
                            .filter(cdsResults -> CollectionUtils.isNotEmpty(cdsResults.results))
                            .flatMap(cdsResults -> cdsResults.results.stream())
                            .filter(csr -> csr.getMatchingRatio() * 100 > pctPositivePixels)
                            .filter(csr -> excludedNames.isEmpty() ||
                                    (!excludedNames.contains(csr.getSourcePublishedName()) && !excludedNames.contains(csr.getPublishedName())))
                            .map(csr -> cleanup ? ColorMIPSearchMatchMetadata.createReleaseCopy(csr) : csr)
                            .collect(Collectors.toList());
                    List<ColorMIPSearchMatchMetadata> combinedResultsWithNoDuplicates = ItemsHandling.selectTopRankedElements(
                            combinedResults,
                            ColorMIPSearchMatchMetadata::getId,
                            ColorMIPSearchMatchMetadata::getNormalizedScore,
                            -1,
                            1)
                            .stream()
                            .flatMap(se -> se.getEntry().stream()).collect(Collectors.toList());

                    if (!combinedResultsWithNoDuplicates.isEmpty()) {
                        ColorMIPSearchResultUtils.sortCDSResults(combinedResultsWithNoDuplicates);
                        ColorMIPSearchResultUtils.writeCDSMatchesToJSONFile(
                                CDSMatches.singletonfromResultsOfColorMIPSearchMatches(combinedResultsWithNoDuplicates),
                                CmdUtils.getOutputFile(outputDir, new File(fn)),
                                args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter());
                    } else {
                        LOG.info("Skip writing {} because it's empty", fn);
                    }
                });
    }

}
