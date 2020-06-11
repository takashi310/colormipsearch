package org.janelia.colormipsearch.cmd;

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
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.colormipsearch.api.GradientAreaGapUtils;
import org.janelia.colormipsearch.api.MaskGradientAreaGapCalculator;
import org.janelia.colormipsearch.api.MaskGradientAreaGapCalculatorProvider;
import org.janelia.colormipsearch.tools.CDSMatches;
import org.janelia.colormipsearch.tools.CachedMIPsUtils;
import org.janelia.colormipsearch.tools.ColorMIPSearchMatchMetadata;
import org.janelia.colormipsearch.tools.ColorMIPSearchResultUtils;
import org.janelia.colormipsearch.tools.MIPImage;
import org.janelia.colormipsearch.tools.MIPMetadata;
import org.janelia.colormipsearch.tools.MIPsUtils;
import org.janelia.colormipsearch.tools.Results;
import org.janelia.colormipsearch.tools.ScoredEntry;
import org.janelia.colormipsearch.tools.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CalculateGradientScoresCmd {
    private static final Logger LOG = LoggerFactory.getLogger(CalculateGradientScoresCmd.class);
    private static final long _1M = 1024 * 1024;

    @Parameters(commandDescription = "Calculate gradient area score for the results")
    static class GradientScoreResultsArgs extends AbstractArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, converter = ListArg.ListArgConverter.class, description = "Results directory to be sorted")
        ListArg resultsDir;

        @Parameter(names = {"--resultsFile", "-rf"}, variableArity = true, converter = ListArg.ListArgConverter.class, description = "File containing results to be sorted")
        List<ListArg> resultsFiles;

        @Parameter(names = {"--topPublishedNameMatches"},
                description = "If set only calculate the gradient score for the specified number of best lines color depth search results")
        int numberOfBestLines;

        @Parameter(names = {"--topPublishedSampleMatches"},
                description = "If set select the specified numnber of best samples for each line to calculate the gradient score")
        int numberOfBestSamplesPerLine = 1;

        @Parameter(names = {"--topMatchesPerSample"},
                description = "Number of best matches for each line to be used for gradient scoring (defaults to 1)")
        int numberOfBestMatchesPerSample = 1;

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

        boolean validate() {
            return resultsDir != null || CollectionUtils.isNotEmpty(resultsFiles);
        }
    }

    private final GradientScoreResultsArgs args;

    CalculateGradientScoresCmd(CommonArgs commonArgs) {
        this.args = new GradientScoreResultsArgs(commonArgs);
    }

    GradientScoreResultsArgs getArgs() {
        return args;
    }

    void execute() {
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
        Map<MIPMetadata, List<ColorMIPSearchMatchMetadata>> resultsGroupedById = selectCDSResultForGradientScoreCalculation(
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

    private Map<MIPMetadata, List<ColorMIPSearchMatchMetadata>> selectCDSResultForGradientScoreCalculation(List<ColorMIPSearchMatchMetadata> cdsResults,
                                                                                                           int numberOfBestLinesToSelect,
                                                                                                           int numberOfBestSamplesToSelectPerLine,
                                                                                                           int numberOfBestMatchesToSelectPerSample) {
        return cdsResults.stream()
                .peek(csr -> csr.setGradientAreaGap(-1))
                .collect(Collectors.groupingBy(csr -> {
                    MIPMetadata mip = new MIPMetadata();
                    mip.setId(csr.getSourceId());
                    mip.setImageArchivePath(csr.getSourceImageArchivePath());
                    mip.setImageName(csr.getSourceImageName());
                    mip.setImageType(csr.getSourceImageType());
                    return mip;
                }, Collectors.collectingAndThen(
                        Collectors.toList(),
                        resultsForAnId -> {
                            List<ColorMIPSearchMatchMetadata> bestMatches = pickBestPublishedNameAndSampleMatches(
                                    resultsForAnId,
                                    numberOfBestLinesToSelect,
                                    numberOfBestSamplesToSelectPerLine,
                                    numberOfBestMatchesToSelectPerSample);
                            LOG.info("Selected {} best matches out of {}", bestMatches.size(), resultsForAnId.size());
                            return bestMatches;
                        })));
    }

    private List<ColorMIPSearchMatchMetadata> pickBestPublishedNameAndSampleMatches(List<ColorMIPSearchMatchMetadata> cdsResults,
                                                                                    int numberOfBestPublishedNamesToSelect,
                                                                                    int numberOfBestSamplesToSelectPerPublishedName,
                                                                                    int numberOfBestMatchesToSelectPerSample) {
        List<ScoredEntry<List<ColorMIPSearchMatchMetadata>>> topResultsByPublishedName = Utils.pickBestMatches(
                cdsResults,
                csr -> StringUtils.defaultIfBlank(csr.getPublishedName(), extractPublishingNameCandidateFromImageName(csr.getImageName())), // pick best results by line
                ColorMIPSearchMatchMetadata::getMatchingPixels,
                numberOfBestPublishedNamesToSelect,
                -1);

        return topResultsByPublishedName.stream()
                .flatMap(se -> Utils.pickBestMatches(
                        se.getEntry(),
                        csr -> csr.getSlideCode(), // pick best results by sample (identified by slide code)
                        ColorMIPSearchMatchMetadata::getMatchingPixels,
                        numberOfBestSamplesToSelectPerPublishedName,
                        numberOfBestMatchesToSelectPerSample <= 0 ? 1 : numberOfBestMatchesToSelectPerSample)
                        .stream())
                .flatMap(se -> se.getEntry().stream())
                .collect(Collectors.toList());
    }

    private String extractPublishingNameCandidateFromImageName(String imageName) {
        String fn = RegExUtils.replacePattern(new File(imageName).getName(), "\\.\\D*$", "");
        int sepIndex = fn.indexOf('_');
        return sepIndex > 0 ? fn.substring(0, sepIndex) : fn;
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
                    MIPImage matchedGradientImage = CachedMIPsUtils.loadMIP(MIPsUtils.getTransformedMIPInfo(matchedMIP, gradientsLocation, gradientSuffix));
                    MIPImage matchedZGapImage = CachedMIPsUtils.loadMIP(MIPsUtils.getTransformedMIPInfo(matchedMIP, zgapsLocation, zgapsSuffix));
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
                        // in this case we could assume the grdients are available for the input and
                        // we could group the results by matched ID but for now we simply do not do it because it's too inefficient.
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
