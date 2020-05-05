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

public class CalculateGradientScoresCmd {
    private static final Logger LOG = LoggerFactory.getLogger(CalculateGradientScoresCmd.class);

    @Parameters(commandDescription = "Calculate gradient area score for the results")
    static class GradientScoreResultsArgs extends AbstractArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, converter = ListArg.ListArgConverter.class, description = "Results directory to be sorted")
        private ListArg resultsDir;

        @Parameter(names = {"--resultsFile", "-rf"}, converter = ListArg.ListArgConverter.class, description = "File containing results to be sorted")
        private ListArg resultsFile;

        @Parameter(names = {"--topPublishedNameMatches"}, description = "If set only calculate the gradient score for the top specified color depth search results")
        private int topPublishedNameMatches;

        @Parameter(names = {"--topPublishedSampleMatches"}, description = "If set select the top sample matches per line to use for gradient score")
        private int topPublishedSampleMatches = 1;

        GradientScoreResultsArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }

        ListArg getResultsDir() {
            return resultsDir;
        }

        ListArg getResultsFile() {
            return resultsFile;
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
        EM2LMAreaGapCalculator emlmAreaGapCalculator = new EM2LMAreaGapCalculator(args.maskThreshold, args.negativeRadius, args.mirrorMask);
        Executor executor = CmdUtils.createCDSExecutor(args);
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        Path outputDir = args.getOutputDir();
        if (args.resultsFile != null) {
            File cdsResultsFile = new File(args.resultsFile.input);
            Results<List<ColorMIPSearchResultMetadata>> cdsResults = calculateGradientAreaScoreForResultsFile(
                    emlmAreaGapCalculator,
                    cdsResultsFile,
                    args.gradientPath,
                    args.zgapPath,
                    StringUtils.defaultString(args.zgapSuffix, ""),
                    args.topPublishedNameMatches,
                    args.topPublishedSampleMatches,
                    mapper,
                    executor
            );
            CmdUtils.writeCDSResultsToJSONFile(cdsResults, CmdUtils.getOutputFile(outputDir, cdsResultsFile), mapper);
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
                                Results<List<ColorMIPSearchResultMetadata>> cdsResults = calculateGradientAreaScoreForResultsFile(
                                        emlmAreaGapCalculator,
                                        f,
                                        args.gradientPath,
                                        args.zgapPath,
                                        StringUtils.defaultString(args.zgapSuffix, ""),
                                        args.topPublishedNameMatches,
                                        args.topPublishedSampleMatches,
                                        mapper,
                                        executor
                                );
                                CmdUtils.writeCDSResultsToJSONFile(cdsResults, CmdUtils.getOutputFile(outputDir, f), mapper);
                            });
                            LOG.info("Finished a batch of {} in {}s", fileList.size(), (System.currentTimeMillis() - startTime) / 1000.);
                        });
            } catch (IOException e) {
                LOG.error("Error listing {}", args.resultsDir, e);
            }
        }
    }

    private Results<List<ColorMIPSearchResultMetadata>> calculateGradientAreaScoreForResultsFile(
            EM2LMAreaGapCalculator emlmAreaGapCalculator,
            File inputResultsFile,
            String gradientsLocation,
            String zgapsLocation,
            String zgapsSuffix,
            int topPublishedNameMatches,
            int topPublishedSampleMatches,
            ObjectMapper mapper,
            Executor executor) {
        Results<List<ColorMIPSearchResultMetadata>> resultsFileContent = CmdUtils.readCDSResultsFromJSONFile(inputResultsFile, mapper);
        if (CollectionUtils.isEmpty(resultsFileContent.results)) {
            LOG.error("No color depth search results found in {}", inputResultsFile);
            return resultsFileContent;
        }
        Map<MIPInfo, List<ColorMIPSearchResultMetadata>> resultsGroupedById = selectCDSResultForGradientScoreCalculation(resultsFileContent.results, topPublishedNameMatches, topPublishedSampleMatches);
        LOG.info("Read {} entries ({} distinct IDs) from {}", resultsFileContent.results.size(), resultsGroupedById.size(), inputResultsFile);

        long startTime = System.currentTimeMillis();
        List<CompletableFuture<List<ColorMIPSearchResultMetadata>>> gradientAreaGapComputations =
                Streams.zip(
                        IntStream.range(0, Integer.MAX_VALUE).boxed(),
                        resultsGroupedById.entrySet().stream(),
                        (i, resultsEntry) -> calculateGradientAreaScoreForCDSResults(
                                inputResultsFile.getAbsolutePath() + "#" + (i + 1),
                                resultsEntry.getKey(),
                                resultsEntry.getValue(),
                                gradientsLocation,
                                zgapsLocation,
                                zgapsSuffix,
                                emlmAreaGapCalculator,
                                executor))
                        .collect(Collectors.toList());
        // wait for all results to complete
        CompletableFuture.allOf(gradientAreaGapComputations.toArray(new CompletableFuture<?>[0])).join();
        LOG.info("Finished gradient area score for {} entries from {} in {}s", gradientAreaGapComputations.size(), inputResultsFile, (System.currentTimeMillis() - startTime) / 1000.);
        CmdUtils.sortCDSResults(resultsFileContent.results);
        LOG.info("Finished sorting by gradient area score for {} entries from {} in {}s", gradientAreaGapComputations.size(), inputResultsFile, (System.currentTimeMillis() - startTime) / 1000.);
        return resultsFileContent;
    }

    private Map<MIPInfo, List<ColorMIPSearchResultMetadata>> selectCDSResultForGradientScoreCalculation(List<ColorMIPSearchResultMetadata> cdsResults, int topPublishedNames, int topSamples) {
        return cdsResults.stream()
                .peek(csr -> csr.setGradientAreaGap(-1))
                .collect(Collectors.groupingBy(csr -> {
                    MIPInfo mip = new MIPInfo();
                    mip.id = csr.id;
                    mip.archivePath = csr.imageArchivePath;
                    mip.imagePath = csr.imageName;
                    mip.type = csr.imageType;
                    return mip;
                }, Collectors.collectingAndThen(
                        Collectors.toList(),
                        resultsForAnId -> {
                            List<ColorMIPSearchResultMetadata> bestMatches = pickBestPublishedNameAndSampleMatches(resultsForAnId, topPublishedNames, topSamples);
                            LOG.info("Selected {} best matches out of {}", bestMatches.size(), resultsForAnId.size());
                            return bestMatches;
                        })));
    }

    private List<ColorMIPSearchResultMetadata> pickBestPublishedNameAndSampleMatches(List<ColorMIPSearchResultMetadata> cdsResults, int topPublishedNames, int topSamples) {
        List<ScoredEntry<List<ColorMIPSearchResultMetadata>>> topResultsByPublishedName = Utils.pickBestMatches(
                cdsResults,
                csr -> StringUtils.defaultIfBlank(csr.matchedPublishedName, extractPublishingNameCandidateFromImageName(csr.matchedImageName)), // pick best results by line
                ColorMIPSearchResultMetadata::getMatchingPixelsPct,
                topPublishedNames,
                -1);

        return topResultsByPublishedName.stream()
                .flatMap(se -> Utils.pickBestMatches(
                        se.entry,
                        csr -> csr.getAttr("Slide Code"), // pick best results by sample (identified by slide code)
                        ColorMIPSearchResultMetadata::getMatchingPixelsPct,
                        topSamples,
                        1).stream())
                .flatMap(se -> se.entry.stream())
                .collect(Collectors.toList());
    }

    private String extractPublishingNameCandidateFromImageName(String imageName) {
        String fn = StringUtils.replacePattern(new File(imageName).getName(), "\\.\\D*$", "");
        int sepIndex = fn.indexOf('_');
        return sepIndex > 0 ? fn.substring(0, sepIndex) : fn;
    }

    private CompletableFuture<List<ColorMIPSearchResultMetadata>> calculateGradientAreaScoreForCDSResults(String resultIDIndex,
                                                                                                          MIPInfo inputMIP,
                                                                                                          List<ColorMIPSearchResultMetadata> selectedCDSResultsForInputMIP,
                                                                                                          String gradientsLocation,
                                                                                                          String zgapsLocation,
                                                                                                          String zgapsSuffix,
                                                                                                          EM2LMAreaGapCalculator emlmAreaGapCalculator,
                                                                                                          Executor executor) {
        LOG.info("Calculate gradient score for {} matches of mip entry# {} - {}", selectedCDSResultsForInputMIP.size(), resultIDIndex, inputMIP);
        long startTimeForCurrentEntry = System.currentTimeMillis();
        CompletableFuture<TriFunction<MIPImage, MIPImage, MIPImage, Long>> gradientGapCalculatorPromise = CompletableFuture.supplyAsync(() -> {
            LOG.info("Load image {}", inputMIP);
            MIPImage inputImage = CachedMIPsUtils.loadMIP(inputMIP);
            return emlmAreaGapCalculator.getGradientAreaCalculator(inputImage);
        }, executor);
        List<CompletableFuture<Long>> areaGapComputations = Streams.zip(
                IntStream.range(0, Integer.MAX_VALUE).boxed(),
                selectedCDSResultsForInputMIP.stream(),
                (i, csr) -> ImmutablePair.of(i + 1, csr))
                .map(indexedCsr -> gradientGapCalculatorPromise.thenApplyAsync(gradientGapCalculator -> {
                    long startGapCalcTime = System.currentTimeMillis();
                    MIPInfo matchedMIP = new MIPInfo();
                    matchedMIP.archivePath = indexedCsr.getRight().matchedImageArchivePath;
                    matchedMIP.imagePath = indexedCsr.getRight().matchedImageName;
                    matchedMIP.type = indexedCsr.getRight().matchedImageType;
                    MIPImage matchedImage = CachedMIPsUtils.loadMIP(matchedMIP);
                    MIPImage matchedGradientImage = CachedMIPsUtils.loadMIP(MIPsUtils.getTransformedMIPInfo(matchedMIP, gradientsLocation, GradientAreaGapUtils.GRADIENT_LOCATION_SUFFIX));
                    MIPImage matchedZGapImage = CachedMIPsUtils.loadMIP(MIPsUtils.getTransformedMIPInfo(matchedMIP, zgapsLocation, zgapsSuffix));
                    LOG.debug("Loaded images for calculating area gap for {}:{} ({} vs {}) in {}ms",
                            resultIDIndex, indexedCsr.getLeft(), inputMIP, matchedMIP, System.currentTimeMillis() - startGapCalcTime);
                    long areaGap;
                    if (matchedImage != null && matchedGradientImage != null) {
                        // only calculate the area gap if the gradient exist
                        LOG.debug("Calculate area gap for {}:{} ({}:{})",
                                resultIDIndex, indexedCsr.getLeft(), inputMIP, matchedMIP);
                        areaGap = gradientGapCalculator.apply(matchedImage, matchedGradientImage, matchedZGapImage);
                        LOG.debug("Finished calculating area gap for {}:{} ({}:{}) in {}ms",
                                resultIDIndex, indexedCsr.getLeft(), inputMIP, matchedMIP, System.currentTimeMillis() - startGapCalcTime);
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
                    LOG.info("Normalize gradient area scores for {} ({})", resultIDIndex, inputMIP);
                    Double maxPctPixelScore = selectedCDSResultsForInputMIP.stream()
                            .map(ColorMIPSearchResultMetadata::getMatchingPixelsPct)
                            .max(Double::compare)
                            .orElse(null);
                    LOG.info("Max pixel percentage score for the {} selected matches of entry# {} ({}) -> {}",
                            selectedCDSResultsForInputMIP.size(), resultIDIndex, inputMIP, maxPctPixelScore);
                    List<Long> areaGaps = areaGapComputations.stream()
                            .map(areaGapComputation -> areaGapComputation.join())
                            .collect(Collectors.toList());
                    long maxAreaGap = areaGaps.stream()
                            .max(Long::compare)
                            .orElse(-1L);
                    LOG.info("Max area gap for the {} selected matches of entry# {} ({}) -> {}",
                            selectedCDSResultsForInputMIP.size(), resultIDIndex, inputMIP, maxAreaGap);
                    // set the normalized area gap values
                    if (maxAreaGap >= 0 && maxPctPixelScore != null) {
                        selectedCDSResultsForInputMIP.stream().filter(csr -> csr.getGradientAreaGap() >= 0)
                                .forEach(csr -> {
                                    csr.setNormalizedGradientAreaGapScore(emlmAreaGapCalculator.calculateAreaGapScore(
                                            csr.getGradientAreaGap(),
                                            maxAreaGap,
                                            csr.getMatchingPixelsPct(),
                                            maxPctPixelScore));
                                });
                    }
                    ;
                    return selectedCDSResultsForInputMIP;
                });
    }

}
