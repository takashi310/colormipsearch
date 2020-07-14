package org.janelia.colormipsearch.cmd;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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
import org.janelia.colormipsearch.api.Utils;
import org.janelia.colormipsearch.api.cdmips.MIPImage;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api.cdmips.MIPsUtils;
import org.janelia.colormipsearch.api.cdsearch.CDSMatches;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchMatchMetadata;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResultUtils;
import org.janelia.colormipsearch.api.gradienttools.GradientAreaGapUtils;
import org.janelia.colormipsearch.api.gradienttools.MaskGradientAreaGapCalculator;
import org.janelia.colormipsearch.api.gradienttools.MaskGradientAreaGapCalculatorProvider;
import org.janelia.colormipsearch.utils.CachedMIPsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CalculateGradientScoresCmd extends AbstractCmd {
    private static final Logger LOG = LoggerFactory.getLogger(CalculateGradientScoresCmd.class);
    private static final long _1M = 1024 * 1024;

    @Parameters(commandDescription = "Calculate gradient area score for the results")
    static class GradientScoreResultsArgs extends AbstractColorDepthMatchArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, converter = ListArg.ListArgConverter.class, description = "Results directory to be sorted")
        ListArg resultsDir;

        @Parameter(names = {"--resultsFile", "-rf"}, variableArity = true, converter = ListArg.ListArgConverter.class, description = "File containing results to be sorted")
        List<ListArg> resultsFiles;

        @Parameter(names = {"--topPublishedNameMatches"},
                description = "If set only calculate the gradient score for the specified number of best lines color depth search results")
        int numberOfBestLines;

        @Parameter(names = {"--topPublishedSampleMatches"},
                description = "If set select the specified numnber of best samples for each line to calculate the gradient score")
        int numberOfBestSamplesPerLine;

        @Parameter(names = {"--topMatchesPerSample"},
                description = "Number of best matches for each line to be used for gradient scoring (defaults to 1)")
        int numberOfBestMatchesPerSample;

        GradientScoreResultsArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }

        Path getOutputDir() {
            if (resultsDir == null && StringUtils.isBlank(commonArgs.outputDir)) {
                return null;
            } else if (StringUtils.isNotBlank(commonArgs.outputDir)) {
                return Paths.get(commonArgs.outputDir);
            } else {
                return Paths.get(resultsDir.input);
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

    private final GradientScoreResultsArgs args;

    CalculateGradientScoresCmd(String commandName, CommonArgs commonArgs) {
        super(commandName);
        this.args = new GradientScoreResultsArgs(commonArgs);
    }

    @Override
    GradientScoreResultsArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        CmdUtils.createOutputDirs(args.getOutputDir());
        calculateGradientAreaScore(args);
    }

    private void calculateGradientAreaScore(GradientScoreResultsArgs args) {
        MaskGradientAreaGapCalculatorProvider maskAreaGapCalculatorProvider =
                MaskGradientAreaGapCalculator.createMaskGradientAreaGapCalculatorProvider(
                        args.maskThreshold, args.negativeRadius, args.mirrorMask
                );
        Executor executor = CmdUtils.createCDSExecutor(args);
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        Path outputDir = args.getOutputDir();
        if (CollectionUtils.isNotEmpty(args.resultsFiles)) {
            Utils.partitionList(args.resultsFiles, args.libraryPartitionSize).stream().parallel()
                    .forEach(inputFiles -> {
                        long startTime = System.currentTimeMillis();
                        inputFiles.forEach(inputArg -> {
                            File f = new File(inputArg.input);
                            CDSMatches cdsMatches = calculateGradientAreaScoreForResultsFile(
                                    maskAreaGapCalculatorProvider,
                                    f,
                                    args.gradientPath,
                                    args.gradientSuffix,
                                    args.zgapPath,
                                    StringUtils.defaultString(args.zgapSuffix, ""),
                                    args.numberOfBestLines,
                                    args.numberOfBestSamplesPerLine,
                                    args.numberOfBestMatchesPerSample,
                                    mapper,
                                    executor
                            );
                            ColorMIPSearchResultUtils.writeCDSMatchesToJSONFile(
                                    cdsMatches,
                                    CmdUtils.getOutputFile(outputDir, f),
                                    args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter());
                        });
                        LOG.info("Finished a batch of {} in {}s - memory usage {}M out of {}M",
                                inputFiles.size(),
                                (System.currentTimeMillis() - startTime) / 1000.,
                                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1, // round up
                                (Runtime.getRuntime().totalMemory() / _1M));
                    });
        } else if (args.resultsDir != null) {
            try {
                int from = Math.max(args.resultsDir.offset, 0);
                int length = args.resultsDir.length;
                List<String> resultFileNames = Files.find(Paths.get(args.resultsDir.input), 1, (p, fa) -> fa.isRegularFile())
                        .skip(from)
                        .map(Path::toString)
                        .collect(Collectors.toList());
                List<String> filesToProcess;
                if (length > 0 && length < resultFileNames.size()) {
                    filesToProcess = resultFileNames.subList(0, length);
                } else {
                    filesToProcess = resultFileNames;
                }
                Utils.partitionList(filesToProcess, args.libraryPartitionSize).stream().parallel()
                        .forEach(fileList -> {
                            long startTime = System.currentTimeMillis();
                            fileList.forEach(fn -> {
                                File f = new File(fn);
                                CDSMatches cdsMatches = calculateGradientAreaScoreForResultsFile(
                                        maskAreaGapCalculatorProvider,
                                        f,
                                        args.gradientPath,
                                        args.gradientSuffix,
                                        args.zgapPath,
                                        StringUtils.defaultString(args.zgapSuffix, ""),
                                        args.numberOfBestLines,
                                        args.numberOfBestSamplesPerLine,
                                        args.numberOfBestMatchesPerSample,
                                        mapper,
                                        executor
                                );
                                ColorMIPSearchResultUtils.writeCDSMatchesToJSONFile(
                                        cdsMatches,
                                        CmdUtils.getOutputFile(outputDir, f),
                                        args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter());
                            });
                            LOG.info("Finished a batch of {} in {}s - memory usage {}M out of {}M",
                                    fileList.size(),
                                    (System.currentTimeMillis() - startTime) / 1000.,
                                    (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1, // round up
                                    (Runtime.getRuntime().totalMemory() / _1M));
                        });
            } catch (IOException e) {
                LOG.error("Error listing {}", args.resultsDir, e);
            }
        }
    }

    private CDSMatches calculateGradientAreaScoreForResultsFile(
            MaskGradientAreaGapCalculatorProvider maskAreaGapCalculatorProvider,
            File inputResultsFile,
            String gradientsLocation,
            String gradientSuffix,
            String zgapsLocation,
            String zgapsSuffix,
            int numberOfBestLinesToSelect,
            int numberOfBestSamplesPerLineToSelect,
            int numberOfBestMatchesPerSampleToSelect,
            ObjectMapper mapper,
            Executor executor) {
        CDSMatches matchesFileContent = ColorMIPSearchResultUtils.readCDSMatchesFromJSONFile(inputResultsFile, mapper);
        if (CollectionUtils.isEmpty(matchesFileContent.results)) {
            LOG.error("No color depth search results found in {}", inputResultsFile);
            return matchesFileContent;
        }
        Map<MIPMetadata, List<ColorMIPSearchMatchMetadata>> resultsGroupedById = ColorMIPSearchResultUtils.selectCDSResultForGradientScoreCalculation(
                matchesFileContent.results,
                numberOfBestLinesToSelect,
                numberOfBestSamplesPerLineToSelect,
                numberOfBestMatchesPerSampleToSelect);
        LOG.info("Read {} entries ({} distinct IDs) from {}", matchesFileContent.results.size(), resultsGroupedById.size(), inputResultsFile);

        long startTime = System.currentTimeMillis();
        List<CompletableFuture<List<ColorMIPSearchMatchMetadata>>> gradientAreaGapComputations =
                Streams.zip(
                        IntStream.range(0, Integer.MAX_VALUE).boxed(),
                        resultsGroupedById.entrySet().stream(),
                        (i, resultsEntry) -> calculateGradientAreaScoreForCDSResults(
                                inputResultsFile.getAbsolutePath() + "#" + (i + 1),
                                resultsEntry.getKey(),
                                resultsEntry.getValue(),
                                gradientsLocation,
                                gradientSuffix,
                                zgapsLocation,
                                zgapsSuffix,
                                maskAreaGapCalculatorProvider,
                                executor))
                        .collect(Collectors.toList());
        // wait for all results to complete
        CompletableFuture.allOf(gradientAreaGapComputations.toArray(new CompletableFuture<?>[0])).join();
        List<ColorMIPSearchMatchMetadata> srWithGradScores = gradientAreaGapComputations.stream()
                .flatMap(gac -> gac.join().stream())
                .collect(Collectors.toList());
        LOG.info("Finished gradient area score for {} out of {} entries from {} in {}s - memory usage {}M",
                srWithGradScores.size(),
                matchesFileContent.results.size(),
                inputResultsFile,
                (System.currentTimeMillis() - startTime) / 1000.,
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1);
        ColorMIPSearchResultUtils.sortCDSResults(srWithGradScores);
        LOG.info("Finished sorting by gradient area score for {} out of {} entries from {} in {}s - memory usage {}M",
                srWithGradScores.size(),
                matchesFileContent.results.size(),
                inputResultsFile,
                (System.currentTimeMillis() - startTime) / 1000.,
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1);
        return CDSMatches.singletonfromResultsOfColorMIPSearchMatches(srWithGradScores);
    }

    private CompletableFuture<List<ColorMIPSearchMatchMetadata>> calculateGradientAreaScoreForCDSResults(String resultIDIndex,
                                                                                                         MIPMetadata inputMaskMIP,
                                                                                                         List<ColorMIPSearchMatchMetadata> selectedCDSResultsForInputMIP,
                                                                                                         String gradientsLocation,
                                                                                                         String gradientSuffix,
                                                                                                         String zgapsLocation,
                                                                                                         String zgapsSuffix,
                                                                                                         MaskGradientAreaGapCalculatorProvider maskAreaGapCalculatorProvider,
                                                                                                         Executor executor) {
        LOG.info("Calculate gradient score for {} matches of mip entry {} - {}", selectedCDSResultsForInputMIP.size(), resultIDIndex, inputMaskMIP);
        CompletableFuture<MaskGradientAreaGapCalculator> gradientGapCalculatorPromise = CompletableFuture.supplyAsync(() -> {
            LOG.info("Load input mask {}", inputMaskMIP);
            MIPImage inputMaskImage = MIPsUtils.loadMIP(inputMaskMIP); // no caching for the mask
            return maskAreaGapCalculatorProvider.createMaskGradientAreaGapCalculator(inputMaskImage.getImageArray());
        }, executor);
        List<CompletableFuture<Long>> areaGapComputations = Streams.zip(
                IntStream.range(0, Integer.MAX_VALUE).boxed(),
                selectedCDSResultsForInputMIP.stream(),
                (i, csr) -> ImmutablePair.of(i + 1, csr))
                .map(indexedCsr -> gradientGapCalculatorPromise.thenApplyAsync(gradientGapCalculator -> {
                    long startGapCalcTime = System.currentTimeMillis();
                    MIPMetadata matchedMIP = new MIPMetadata();
                    matchedMIP.setImageArchivePath(indexedCsr.getRight().getImageArchivePath());
                    matchedMIP.setImageName(indexedCsr.getRight().getImageName());
                    matchedMIP.setImageType(indexedCsr.getRight().getImageType());
                    MIPImage matchedImage = CachedMIPsUtils.loadMIP(matchedMIP);
                    MIPImage matchedGradientImage = CachedMIPsUtils.loadMIP(MIPsUtils.getAncillaryMIPInfo(matchedMIP, gradientsLocation, gradientSuffix));
                    MIPImage matchedZGapImage = CachedMIPsUtils.loadMIP(MIPsUtils.getAncillaryMIPInfo(matchedMIP, zgapsLocation, zgapsSuffix));
                    LOG.debug("Loaded images for calculating area gap for {}:{} ({} vs {}) in {}ms",
                            resultIDIndex, indexedCsr.getLeft(), inputMaskMIP, matchedMIP, System.currentTimeMillis() - startGapCalcTime);
                    long areaGap;
                    if (matchedImage != null && matchedGradientImage != null) {
                        // only calculate the area gap if the gradient exist
                        LOG.debug("Calculate area gap for {}:{} ({}:{})",
                                resultIDIndex, indexedCsr.getLeft(), inputMaskMIP, matchedMIP);
                        areaGap = gradientGapCalculator.calculateMaskAreaGap(
                                matchedImage.getImageArray(),
                                matchedGradientImage.getImageArray(),
                                matchedZGapImage != null ? matchedZGapImage.getImageArray() : null);
                        LOG.debug("Finished calculating area gap for {}:{} ({}:{}) in {}ms",
                                resultIDIndex, indexedCsr.getLeft(), inputMaskMIP, matchedMIP, System.currentTimeMillis() - startGapCalcTime);
                    } else {
                        areaGap = -1;
                    }
                    indexedCsr.getRight().setGradientAreaGap(areaGap);
                    return areaGap;
                }, executor))
                .collect(Collectors.toList());
        return CompletableFuture.allOf(areaGapComputations.toArray(new CompletableFuture<?>[0]))
                .thenApply(vr -> {
                    LOG.info("Normalize gradient area scores for {} ({})", resultIDIndex, inputMaskMIP);
                    Integer maxMatchingPixels = selectedCDSResultsForInputMIP.stream()
                            .map(ColorMIPSearchMatchMetadata::getMatchingPixels)
                            .max(Integer::compare)
                            .orElse(0);
                    LOG.info("Max pixel percentage score for the {} selected matches of entry {} ({}) -> {}",
                            selectedCDSResultsForInputMIP.size(), resultIDIndex, inputMaskMIP, maxMatchingPixels);
                    List<Long> areaGaps = areaGapComputations.stream()
                            .map(areaGapComputation -> areaGapComputation.join())
                            .collect(Collectors.toList());
                    long maxAreaGap = areaGaps.stream()
                            .max(Long::compare)
                            .orElse(-1L);
                    LOG.info("Max area gap for the {} selected matches of entry {} ({}) -> {}",
                            selectedCDSResultsForInputMIP.size(), resultIDIndex, inputMaskMIP, maxAreaGap);
                    // set the normalized area gap values
                    if (maxAreaGap >= 0 && maxMatchingPixels > 0) {
                        selectedCDSResultsForInputMIP.stream().filter(csr -> csr.getGradientAreaGap() >= 0)
                                .forEach(csr -> {
                                    csr.setNormalizedGapScore(GradientAreaGapUtils.calculateAreaGapScore(
                                            csr.getGradientAreaGap(),
                                            maxAreaGap,
                                            csr.getMatchingPixels(),
                                            csr.getMatchingRatio(),
                                            maxMatchingPixels));
                                });
                    }
                    ;
                    return selectedCDSResultsForInputMIP;
                });
    }

}
