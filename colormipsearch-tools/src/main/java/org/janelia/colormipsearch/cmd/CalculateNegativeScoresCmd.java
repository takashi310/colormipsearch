package org.janelia.colormipsearch.cmd;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.janelia.colormipsearch.api.imageprocessing.ImageArray;
import org.janelia.colormipsearch.utils.CachedMIPsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CalculateNegativeScoresCmd extends AbstractCmd {
    private static final Logger LOG = LoggerFactory.getLogger(CalculateNegativeScoresCmd.class);

    @Parameters(commandDescription = "Calculate gradient area score for the results")
    static class NegativeScoreResultsArgs extends AbstractColorDepthMatchArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, converter = ListArg.ListArgConverter.class, description = "Results directory to be sorted")
        ListArg resultsDir;

        @Parameter(names = {"--resultsFile", "-rf"}, variableArity = true, description = "File(s) containing results to be calculated")
        private List<String> resultsFiles;

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
                        args.mirrorMask,
                        args.negativeRadius,
                        loadQueryROIMask(args.queryROIMaskName));
        Executor executor = CmdUtils.createCDSExecutor(args.commonArgs);
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        List<String> filesToProcess;
        if (CollectionUtils.isNotEmpty(args.resultsFiles)) {
            filesToProcess = args.resultsFiles;
        } else if (args.resultsDir != null) {
            filesToProcess = CmdUtils.getFileToProcessFromDir(args.resultsDir.input, args.resultsDir.offset, args.resultsDir.length);
        } else {
            filesToProcess = Collections.emptyList();
        }
        int nFiles = filesToProcess.size();
        Path outputDir = args.getOutputDir();
        long startProcessing = System.currentTimeMillis();
        List<CompletableFuture<Void>> allGradScoreComputations = Utils.partitionCollection(filesToProcess, args.processingPartitionSize).stream().parallel()
                .map(fileList -> CompletableFuture.supplyAsync(() -> {
                            long startProcessingPartition = System.currentTimeMillis();
                            List<CompletableFuture<File>> gradScoreComputations = fileList.stream()
                                    .map(filename -> gradientAreaScoreComputationForResultsFile(
                                            negativeMatchCDSArgorithmProvider,
                                            new File(filename),
                                            args.librarySuffix,
                                            args.gradientPaths,
                                            args.gradientSuffix,
                                            args.zgapPaths,
                                            StringUtils.defaultString(args.zgapSuffix, ""),
                                            args.numberOfBestLines,
                                            args.numberOfBestSamplesPerLine,
                                            args.numberOfBestMatchesPerSample,
                                            outputDir,
                                            mapper,
                                            executor
                                    ))
                                    .collect(Collectors.toList());
                            return CompletableFuture.allOf(gradScoreComputations.toArray(new CompletableFuture<?>[0]))
                                    .thenAccept(ignored -> {
                                        LOG.info("Finished a batch of {} in {}s - memory usage {}M out of {}M",
                                                fileList.size(),
                                                (System.currentTimeMillis() - startProcessingPartition) / 1000.,
                                                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1, // round up
                                                (Runtime.getRuntime().totalMemory() / _1M));

                                    })
                                    .join();
                        }))
                .collect(Collectors.toList())
                ;
        CompletableFuture.allOf(allGradScoreComputations.toArray(new CompletableFuture<?>[0]))
                .thenAccept(ignored -> {
                    LOG.info("Finished gradient scores for {} in {}s - memory usage {}M out of {}M",
                            nFiles,
                            (System.currentTimeMillis() - startProcessing) / 1000.,
                            (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1, // round up
                            (Runtime.getRuntime().totalMemory() / _1M));

                })
                .join();
    }

    private ImageArray<?> loadQueryROIMask(String queryROIMask) {
        if (StringUtils.isBlank(queryROIMask)) {
            return null;
        } else {
            List<MIPMetadata> queryROIMIPs = MIPsUtils.readMIPsFromLocalFiles(queryROIMask, 0, 1, Collections.emptySet());
            return queryROIMIPs.stream()
                    .findFirst()
                    .map(MIPsUtils::loadMIP)
                    .map(MIPImage::getImageArray)
                    .orElse(null);
        }
    }

    private CompletableFuture<File> gradientAreaScoreComputationForResultsFile(
            ColorDepthSearchAlgorithmProvider<NegativeColorDepthMatchScore> negativeMatchCDSArgorithmProvider,
            File inputResultsFile,
            String targetSuffix,
            List<String> gradientsLocations,
            String gradientSuffix,
            List<String> zgapsLocations,
            String zgapsSuffix,
            int numberOfBestLinesToSelect,
            int numberOfBestSamplesPerLineToSelect,
            int numberOfBestMatchesPerSampleToSelect,
            Path outputDir,
            ObjectMapper mapper,
            Executor executor) {
        CDSMatches matchesFileContent = ColorMIPSearchResultUtils.readCDSMatchesFromJSONFile(inputResultsFile, mapper);
        if (CollectionUtils.isEmpty(matchesFileContent.results)) {
            LOG.error("No color depth search results found in {}", inputResultsFile);
            return CompletableFuture.completedFuture(null);
        }
        LOG.info("Select best matches: {}", numberOfBestLinesToSelect);
        Map<MIPMetadata, List<ColorMIPSearchMatchMetadata>> resultsGroupedByQuery = ColorMIPSearchResultUtils.selectCDSResultForGradientScoreCalculation(
                matchesFileContent.results,
                numberOfBestLinesToSelect,
                numberOfBestSamplesPerLineToSelect,
                numberOfBestMatchesPerSampleToSelect);
        LOG.info("Read {} entries ({} distinct mask MIPs) from {}", matchesFileContent.results.size(), resultsGroupedByQuery.size(), inputResultsFile);

        String cdsResultsSource = inputResultsFile.getAbsolutePath();
        long startTime = System.currentTimeMillis();
        List<CompletableFuture<List<ColorMIPSearchMatchMetadata>>> negativeScoresComputations =
                resultsGroupedByQuery.entrySet().stream().parallel()
                        .map(queryResultsEntry -> {
                            long negativeComputationsStartTime = System.currentTimeMillis();
                            List<CompletableFuture<Long>> negativeScoreComputations = createNegativeScoreComputations(
                                    cdsResultsSource,
                                    queryResultsEntry.getKey(),
                                    queryResultsEntry.getValue(),
                                    targetSuffix,
                                    gradientsLocations,
                                    gradientSuffix,
                                    zgapsLocations,
                                    zgapsSuffix,
                                    negativeMatchCDSArgorithmProvider,
                                    executor);
                            return collectNegativeScores(
                                    cdsResultsSource,
                                    queryResultsEntry.getKey(),
                                    queryResultsEntry.getValue(),
                                    negativeScoreComputations,
                                    negativeComputationsStartTime,
                                    executor);
                        })
                        .collect(Collectors.toList());
        return CompletableFuture.allOf(negativeScoresComputations.toArray(new CompletableFuture<?>[0]))
                .thenApply(ignoredVoidResult -> negativeScoresComputations.stream()
                        .flatMap(gsc -> gsc.join().stream())
                        .collect(Collectors.toList()))
                .thenApply(srWithNegativeScores -> {
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
                    CDSMatches cdsMatches = CDSMatches.singletonfromResultsOfColorMIPSearchMatches(srWithNegativeScores);
                    File outputFile = CmdUtils.getOutputFile(outputDir, inputResultsFile);
                    ColorMIPSearchResultUtils.writeCDSMatchesToJSONFile(
                            cdsMatches,
                            outputFile,
                            args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter());
                    return outputFile;
                })
                ;
    }

    private List<CompletableFuture<Long>> createNegativeScoreComputations(String cdsMatchesSource,
                                                                          MIPMetadata inputQueryMIP,
                                                                          List<ColorMIPSearchMatchMetadata> selectedCDSResultsForQueryMIP,
                                                                          String targetSuffix,
                                                                          List<String> gradientsLocations,
                                                                          String gradientSuffix,
                                                                          List<String> zgapsLocations,
                                                                          String zgapsSuffix,
                                                                          ColorDepthSearchAlgorithmProvider<NegativeColorDepthMatchScore> negativeMatchCDSArgorithmProvider,
                                                                          Executor executor) {
        LOG.info("Prepare gradient score computations for {} with {} entries from {}", inputQueryMIP, selectedCDSResultsForQueryMIP.size(), cdsMatchesSource);
        LOG.info("Load query image {}", inputQueryMIP);
        MIPImage inputQueryImage = MIPsUtils.loadMIP(inputQueryMIP); // no caching for the mask
        if (inputQueryImage == null) {
            LOG.error("No image found for {}", inputQueryMIP);
            return Collections.emptyList();
        }
        ColorDepthSearchAlgorithm<NegativeColorDepthMatchScore> gradientScoreCalculator =
                negativeMatchCDSArgorithmProvider.createColorDepthQuerySearchAlgorithmWithDefaultParams(inputQueryImage.getImageArray(), args.maskThreshold);
        return selectedCDSResultsForQueryMIP.stream()
                .map(csr -> CompletableFuture.supplyAsync(() -> {
                    long startGapCalcTime = System.currentTimeMillis();
                    Set<String> requiredVariantTypes = gradientScoreCalculator.getRequiredTargetVariantTypes();
                    MIPMetadata matchedMIP = ColorMIPSearchMatchMetadata.getTargetMIP(csr);
                    MIPImage matchedImage = CachedMIPsUtils.loadMIP(matchedMIP);
                    LOG.debug("Processing {} - loaded MIP {} for calculating area gap in {}ms",
                            cdsMatchesSource, inputQueryMIP, System.currentTimeMillis() - startGapCalcTime);
                    long negativeScore;
                    if (matchedImage != null) {
                        // only calculate the area gap if the gradient exist
                        LOG.debug("Processing {} - calculate negative score between {} and {}",
                                cdsMatchesSource, inputQueryMIP, matchedMIP);
                        Map<String, Supplier<ImageArray<?>>> variantImageSuppliers = new HashMap<>();
                        if (requiredVariantTypes.contains("gradient")) {
                            variantImageSuppliers.put("gradient", createVariantImageSupplier(matchedMIP, "gradient", gradientsLocations, gradientSuffix, targetSuffix));
                        }
                        if (requiredVariantTypes.contains("zgap")) {
                            variantImageSuppliers.put("zgap", createVariantImageSupplier(matchedMIP, "zgap", zgapsLocations, zgapsSuffix, targetSuffix));
                        }
                        NegativeColorDepthMatchScore negativeScores = gradientScoreCalculator.calculateMatchingScore(
                                MIPsUtils.getImageArray(matchedImage),
                                variantImageSuppliers);
                        csr.setGradientAreaGap(negativeScores.getGradientAreaGap());
                        csr.setHighExpressionArea(negativeScores.getHighExpressionArea());
                        LOG.debug("Processing {} - finished calculating negative score between {} and {} in {}ms",
                                cdsMatchesSource, inputQueryMIP, matchedMIP, System.currentTimeMillis() - startGapCalcTime);
                        negativeScore = negativeScores.getScore();
                    } else {
                        csr.setGradientAreaGap(-1);
                        csr.setHighExpressionArea(-1);
                        negativeScore = -1;
                    }
                    return negativeScore;
                }, executor))
                .collect(Collectors.toList());
    }

    private Supplier<ImageArray<?>> createVariantImageSupplier(MIPMetadata mip,
                                                               String variant,
                                                               List<String> variantLocations,
                                                               String variantTypeSuffix,
                                                               String replacedSuffix) {
        return  () -> {
            MIPImage variantImage = CachedMIPsUtils.loadMIP(MIPsUtils.getMIPVariantInfo(
                    mip,
                    variant,
                    variantLocations,
                    nc -> {
                        String suffix = StringUtils.defaultIfBlank(variantTypeSuffix, "");
                        if (StringUtils.isNotBlank(replacedSuffix)) {
                            return StringUtils.replaceIgnoreCase(nc, replacedSuffix, "") + suffix;
                        } else {
                            return nc + suffix;
                        }
                    },
                    null));
            return MIPsUtils.getImageArray(variantImage);
        };
    }

    private CompletableFuture<List<ColorMIPSearchMatchMetadata>> collectNegativeScores(String cdsMatchesSource,
                                                                                       MIPMetadata inputQueryMIP,
                                                                                       List<ColorMIPSearchMatchMetadata> selectedCDSResultsForQueryMIP,
                                                                                       List<CompletableFuture<Long>> negativeScoresComputations,
                                                                                       long startProcessingTime,
                                                                                       Executor executor) {
        LOG.info("Wait for all gradient computations for {} with {} matches from {} to finish to normalize score", inputQueryMIP, selectedCDSResultsForQueryMIP.size(), cdsMatchesSource);
        return CompletableFuture.allOf(negativeScoresComputations.toArray(new CompletableFuture<?>[0]))
                .thenApplyAsync(vr -> {
                    LOG.info("Normalize gradient area scores for {} matches with {} from {}", selectedCDSResultsForQueryMIP.size(), inputQueryMIP, cdsMatchesSource);
                    List<Long> negativeScores = negativeScoresComputations.stream()
                            .map(CompletableFuture::join)
                            .collect(Collectors.toList());
                    long maxNegativeScore = negativeScores.stream()
                            .max(Long::compare)
                            .orElse(-1L);
                    LOG.info("Max negative score for {} matches with {} from {} -> {}",
                            selectedCDSResultsForQueryMIP.size(), inputQueryMIP, cdsMatchesSource, maxNegativeScore);
                    Integer maxMatchingPixels = selectedCDSResultsForQueryMIP.stream()
                            .map(ColorMIPSearchMatchMetadata::getMatchingPixels)
                            .max(Integer::compare)
                            .orElse(0);
                    LOG.info("Max pixel score for {} matches with {} from {} -> {}",
                            selectedCDSResultsForQueryMIP.size(), inputQueryMIP, cdsMatchesSource, maxMatchingPixels);
                    selectedCDSResultsForQueryMIP.stream()
                            .filter(csr -> csr.getGradientAreaGap() >= 0)
                            .forEach(csr -> {
                                csr.setNormalizedGapScore(GradientAreaGapUtils.calculateNormalizedScore(
                                        csr.getMatchingPixels(),
                                        csr.getGradientAreaGap(),
                                        csr.getHighExpressionArea(),
                                        maxMatchingPixels,
                                        maxNegativeScore
                                ));
                            });
                    LOG.info("Updated normalized score for {} matches with {} from {} after {}s",
                            selectedCDSResultsForQueryMIP.size(), inputQueryMIP, cdsMatchesSource, (System.currentTimeMillis()-startProcessingTime+1000)/1000);
                    return selectedCDSResultsForQueryMIP;
                }, executor);
    }
}
