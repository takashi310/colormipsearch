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
import java.util.function.Supplier;
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
import org.janelia.colormipsearch.api.cdsearch.ColorDepthSearchAlgorithm;
import org.janelia.colormipsearch.api.cdsearch.ColorDepthSearchAlgorithmProvider;
import org.janelia.colormipsearch.api.cdsearch.ColorDepthSearchAlgorithmProviderFactory;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchMatchMetadata;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResultUtils;
import org.janelia.colormipsearch.api.cdsearch.GradientAreaGapUtils;
import org.janelia.colormipsearch.api.cdsearch.NegativeColorDepthMatchScore;
import org.janelia.colormipsearch.utils.CachedMIPsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CalculateNegativeScoresCmd extends AbstractCmd {
    private static final Logger LOG = LoggerFactory.getLogger(CalculateNegativeScoresCmd.class);

    @Parameters(commandDescription = "Calculate gradient area score for the results")
    static class NegativeScoreResultsArgs extends AbstractColorDepthMatchArgs {
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

        NegativeScoreResultsArgs(CommonArgs commonArgs) {
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

    private final NegativeScoreResultsArgs args;
    private final Supplier<Long> cacheSizeSupplier;
    private final Supplier<Long> cacheExpirationInSecondsSupplier;

    CalculateNegativeScoresCmd(String commandName,
                               CommonArgs commonArgs,
                               Supplier<Long> cacheSizeSupplier,
                               Supplier<Long> cacheExpirationInSecondsSupplier) {
        super(commandName);
        this.args = new NegativeScoreResultsArgs(commonArgs);
        this.cacheSizeSupplier = cacheSizeSupplier;
        this.cacheExpirationInSecondsSupplier = cacheExpirationInSecondsSupplier;
    }

    @Override
    NegativeScoreResultsArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        CmdUtils.createOutputDirs(args.getOutputDir());
        // initialize the cache
        CachedMIPsUtils.initializeCache(cacheSizeSupplier.get(), cacheExpirationInSecondsSupplier.get());
        calculateGradientAreaScore(args);
    }

    private void calculateGradientAreaScore(NegativeScoreResultsArgs args) {
        ColorDepthSearchAlgorithmProvider<NegativeColorDepthMatchScore> negativeMatchCDSArgorithmProvider =
                ColorDepthSearchAlgorithmProviderFactory.createNegativeMatchCDSAlgorithmProvider(
                        args.maskThreshold, args.negativeRadius, args.mirrorMask
                );
        Executor executor = CmdUtils.createCDSExecutor(args.commonArgs);
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
                                    negativeMatchCDSArgorithmProvider,
                                    f,
                                    args.librarySuffix,
                                    args.gradientPaths,
                                    args.gradientSuffix,
                                    args.zgapPaths,
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
                                        negativeMatchCDSArgorithmProvider,
                                        f,
                                        args.librarySuffix,
                                        args.gradientPaths,
                                        args.gradientSuffix,
                                        args.zgapPaths,
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
            ColorDepthSearchAlgorithmProvider<NegativeColorDepthMatchScore> negativeMatchCDSArgorithmProvider,
            File inputResultsFile,
            String librarySuffix,
            List<String> gradientsLocations,
            String gradientSuffix,
            List<String> zgapsLocations,
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
        LOG.info("Read {} entries ({} distinct mask MIPs) from {}", matchesFileContent.results.size(), resultsGroupedById.size(), inputResultsFile);

        long startTime = System.currentTimeMillis();
        List<CompletableFuture<List<ColorMIPSearchMatchMetadata>>> negativeScoresComputations =
                Streams.zip(
                        IntStream.range(0, Integer.MAX_VALUE).boxed(),
                        resultsGroupedById.entrySet().stream(),
                        (i, resultsEntry) -> calculateNegativeScoresForCDSResults(
                                inputResultsFile.getAbsolutePath() + "#" + (i + 1),
                                resultsEntry.getKey(),
                                resultsEntry.getValue(),
                                librarySuffix,
                                gradientsLocations,
                                gradientSuffix,
                                zgapsLocations,
                                zgapsSuffix,
                                negativeMatchCDSArgorithmProvider,
                                executor))
                        .collect(Collectors.toList());
        // wait for all results to complete
        CompletableFuture.allOf(negativeScoresComputations.toArray(new CompletableFuture<?>[0])).join();
        List<ColorMIPSearchMatchMetadata> srWithNegativeScores = negativeScoresComputations.stream()
                .flatMap(gac -> gac.join().stream())
                .collect(Collectors.toList());
        LOG.info("Finished gradient area score for {} out of {} entries from {} in {}s - memory usage {}M",
                srWithNegativeScores.size(),
                matchesFileContent.results.size(),
                inputResultsFile,
                (System.currentTimeMillis() - startTime) / 1000.,
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1);
        ColorMIPSearchResultUtils.sortCDSResults(srWithNegativeScores);
        LOG.info("Finished sorting by gradient area score for {} out of {} entries from {} in {}s - memory usage {}M",
                srWithNegativeScores.size(),
                matchesFileContent.results.size(),
                inputResultsFile,
                (System.currentTimeMillis() - startTime) / 1000.,
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1);
        return CDSMatches.singletonfromResultsOfColorMIPSearchMatches(srWithNegativeScores);
    }

    private CompletableFuture<List<ColorMIPSearchMatchMetadata>> calculateNegativeScoresForCDSResults(String resultIDIndex,
                                                                                                      MIPMetadata inputMaskMIP,
                                                                                                      List<ColorMIPSearchMatchMetadata> selectedCDSResultsForInputMIP,
                                                                                                      String librarySuffix,
                                                                                                      List<String> gradientsLocations,
                                                                                                      String gradientSuffix,
                                                                                                      List<String> zgapsLocations,
                                                                                                      String zgapsSuffix,
                                                                                                      ColorDepthSearchAlgorithmProvider<NegativeColorDepthMatchScore> negativeMatchCDSArgorithmProvider,
                                                                                                      Executor executor) {
        LOG.info("Calculate gradient score for {} matches of mip entry {} - {}", selectedCDSResultsForInputMIP.size(), resultIDIndex, inputMaskMIP);
        CompletableFuture<ColorDepthSearchAlgorithm<NegativeColorDepthMatchScore>> gradientGapCalculatorPromise = CompletableFuture.supplyAsync(() -> {
            LOG.info("Load input mask {}", inputMaskMIP);
            MIPImage inputMaskImage = MIPsUtils.loadMIP(inputMaskMIP); // no caching for the mask
            return negativeMatchCDSArgorithmProvider.createColorDepthQuerySearchAlgorithmWithDefaultParams(inputMaskImage.getImageArray());
        }, executor);
        List<CompletableFuture<Long>> negativeScoresComputations = Streams.zip(
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
                    MIPImage matchedGradientImage = CachedMIPsUtils.loadMIP(MIPsUtils.getMIPVariantInfo(
                            matchedMIP,
                            gradientsLocations,
                            nc -> {
                                String suffix = StringUtils.defaultIfBlank(gradientSuffix, "");
                                if (StringUtils.isNotBlank(librarySuffix)) {
                                    return StringUtils.replaceIgnoreCase(nc, librarySuffix, "") + suffix;
                                } else {
                                    return nc + suffix;
                                }
                            }));
                    MIPImage matchedZGapImage = CachedMIPsUtils.loadMIP(MIPsUtils.getMIPVariantInfo(
                            matchedMIP,
                            zgapsLocations,
                            nc -> {
                                String suffix = StringUtils.defaultIfBlank(zgapsSuffix, "");
                                if (StringUtils.isNotBlank(librarySuffix)) {
                                    return StringUtils.replaceIgnoreCase(nc, librarySuffix, "") + suffix;
                                } else {
                                    return nc + suffix;
                                }
                            }));
                    LOG.debug("Loaded images for calculating area gap for {}:{} ({} vs {}) in {}ms",
                            resultIDIndex, indexedCsr.getLeft(), inputMaskMIP, matchedMIP, System.currentTimeMillis() - startGapCalcTime);
                    long negativeScore;
                    if (matchedImage != null && matchedGradientImage != null) {
                        // only calculate the area gap if the gradient exist
                        LOG.debug("Calculate area gap for {}:{} ({}:{})",
                                resultIDIndex, indexedCsr.getLeft(), inputMaskMIP, matchedMIP);
                        NegativeColorDepthMatchScore negativeScores = gradientGapCalculator.calculateMatchingScore(
                                MIPsUtils.getImageArray(matchedImage),
                                MIPsUtils.getImageArray(matchedGradientImage),
                                MIPsUtils.getImageArray(matchedZGapImage));
                        indexedCsr.getRight().setGradientAreaGap(negativeScores.getGradientAreaGap());
                        indexedCsr.getRight().setHighExpressionArea(negativeScores.getHighExpressionArea());
                        LOG.debug("Finished calculating area gap for {}:{} ({}:{}) in {}ms",
                                resultIDIndex, indexedCsr.getLeft(), inputMaskMIP, matchedMIP, System.currentTimeMillis() - startGapCalcTime);
                        negativeScore = negativeScores.getScore();
                    } else {
                        indexedCsr.getRight().setGradientAreaGap(-1);
                        indexedCsr.getRight().setHighExpressionArea(-1);
                        negativeScore = -1;
                    }
                    return negativeScore;
                }, executor))
                .collect(Collectors.toList());
        return CompletableFuture.allOf(negativeScoresComputations.toArray(new CompletableFuture<?>[0]))
                .thenApply(vr -> {
                    LOG.info("Normalize gradient area scores for {} ({})", resultIDIndex, inputMaskMIP);
                    Integer maxMatchingPixels = selectedCDSResultsForInputMIP.stream()
                            .map(ColorMIPSearchMatchMetadata::getMatchingPixels)
                            .max(Integer::compare)
                            .orElse(0);
                    LOG.info("Max pixel percentage score for the {} selected matches of entry {} ({}) -> {}",
                            selectedCDSResultsForInputMIP.size(), resultIDIndex, inputMaskMIP, maxMatchingPixels);
                    List<Long> negativeScores = negativeScoresComputations.stream()
                            .map(CompletableFuture::join)
                            .collect(Collectors.toList());
                    long maxNegativeScore = negativeScores.stream()
                            .max(Long::compare)
                            .orElse(-1L);
                    LOG.info("Max negative score for the {} selected matches of entry {} ({}) -> {}",
                            selectedCDSResultsForInputMIP.size(), resultIDIndex, inputMaskMIP, maxNegativeScore);
                    // set the normalized area gap values
                    if (maxNegativeScore >= 0 && maxMatchingPixels > 0) {
                        selectedCDSResultsForInputMIP.stream().filter(csr -> csr.getGradientAreaGap() >= 0)
                                .forEach(csr -> {
                                    csr.setNormalizedGapScore(GradientAreaGapUtils.calculateNormalizedScore(
                                            csr.getGradientAreaGap(),
                                            csr.getHighExpressionArea(),
                                            maxNegativeScore,
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
