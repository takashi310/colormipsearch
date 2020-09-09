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
import org.janelia.colormipsearch.api.gradienttools.GradientAreaGapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NormalizeScoresCmd extends AbstractCmd {
    private static final Logger LOG = LoggerFactory.getLogger(NormalizeScoresCmd.class);

    @Parameters(commandDescription = "Normalize gradient score for the search results - " +
            "if the area gap is not available consider it as if there was a perfect shape match, i.e., areagap = 0")
    static class NormalizeGradientScoresArgs extends AbstractCmdArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, converter = ListArg.ListArgConverter.class,
                description = "Results directory for which scores will be normalized")
        private ListArg resultsDir;

        @Parameter(names = {"--resultsFile", "-rf"}, variableArity = true,
                description = "Files for which results will be normalized")
        private List<String> resultsFiles;

        @Parameter(names = {"--pctPositivePixels"}, description = "% of Positive PX Threshold (0-100%)")
        private Double pctPositivePixels = 0.0;

        @Parameter(names = "-cleanup", description = "Cleanup results and remove fields not necessary in productiom", arity = 0)
        private boolean cleanup = false;

        @Parameter(names = "--re-normalize",
                description = "If set (re)normalize the gradient scores based on the new dataset",
                arity = 0)
        private boolean reNormalize = false;

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

    private final NormalizeGradientScoresArgs args;
    private final ObjectMapper mapper;

    NormalizeScoresCmd(String commandName, CommonArgs commonArgs) {
        super(commandName);
        args = new NormalizeGradientScoresArgs(commonArgs);
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    NormalizeGradientScoresArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        CmdUtils.createOutputDirs(args.getOutputDir());
        setGradientScores(args);
    }

    private void setGradientScores(NormalizeGradientScoresArgs args) {
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
        filesToProcess.stream().parallel().forEach((fn) -> {
            LOG.info("Set gradient score results for {}", fn);
            File cdsFile = new File(fn);
            CDSMatches cdsMatchesFromJSONFile = ColorMIPSearchResultUtils.readCDSMatchesFromJSONFile(cdsFile, mapper);
            List<ColorMIPSearchMatchMetadata> cdsResults;
            if (args.cleanup) {
                cdsResults = Utils.pickBestMatches(
                        cdsMatchesFromJSONFile.results,
                        csr -> csr.getId(),
                        csr -> (double) csr.getMatchingPixels(),
                        -1,
                        1)
                        .stream()
                        .flatMap(se -> se.getEntry().stream()).collect(Collectors.toList());
            } else {
                cdsResults = cdsMatchesFromJSONFile.results;
            }
            long maxAreaGap;
            int maxMatchingPixels;
            if (args.reNormalize) {
                maxAreaGap = cdsResults.stream().parallel()
                        .map(ColorMIPSearchMatchMetadata::getGradientAreaGap)
                        .max(Long::compare)
                        .orElse(-1L);
                LOG.info("Max area gap for {}  -> {}", fn, maxAreaGap);
                maxMatchingPixels = cdsResults.stream().parallel()
                        .map(ColorMIPSearchMatchMetadata::getMatchingPixels)
                        .max(Integer::compare)
                        .orElse(0);
                LOG.info("Max pixel match for {}  -> {}", fn, maxMatchingPixels);
            } else {
                maxAreaGap = -1L;
                maxMatchingPixels = -1;
            }
            List<ColorMIPSearchMatchMetadata> cdsResultsWithNormalizedScore = cdsResults.stream().parallel()
                    .filter(csr -> csr.getMatchingRatio() * 100. > args.pctPositivePixels)
                    .map(csr -> args.cleanup ? ColorMIPSearchMatchMetadata.create(csr) : csr)
                    .peek(csr -> {
                        if (args.reNormalize) {
                            long areaGap = csr.getGradientAreaGap();
                            double normalizedGapScore = GradientAreaGapUtils.calculateAreaGapScore(
                                    csr.getGradientAreaGap(), maxAreaGap, csr.getMatchingPixels(), csr.getMatchingRatio(), maxMatchingPixels
                            );
                            if (areaGap >= 0) {
                                LOG.debug("Update normalized score for {}: matchingPixels {}, matchingPixelsToMaskRatio {}, areaGap {}, current score {} -> updated score {}",
                                        csr, csr.getMatchingPixels(), csr.getMatchingRatio(), areaGap, csr.getNormalizedGapScore(), normalizedGapScore);
                                csr.setNormalizedGapScore(normalizedGapScore);
                            } else {
                                csr.setArtificialShapeScore(normalizedGapScore);
                            }
                        }
                    })
                    .collect(Collectors.toList());
            ColorMIPSearchResultUtils.sortCDSResults(cdsResultsWithNormalizedScore);
            ColorMIPSearchResultUtils.writeCDSMatchesToJSONFile(
                    CDSMatches.singletonfromResultsOfColorMIPSearchMatches(cdsResultsWithNormalizedScore),
                    CmdUtils.getOutputFile(args.getOutputDir(), new File(fn)),
                    args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter());
        });
    }

}
