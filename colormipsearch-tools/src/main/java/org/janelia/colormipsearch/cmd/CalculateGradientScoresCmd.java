package org.janelia.colormipsearch.cmd;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.colormipsearch.cds.ColorDepthSearchAlgorithm;
import org.janelia.colormipsearch.cds.ColorDepthSearchAlgorithmProvider;
import org.janelia.colormipsearch.cds.ColorDepthSearchAlgorithmProviderFactory;
import org.janelia.colormipsearch.cds.CombinedMatchScore;
import org.janelia.colormipsearch.cds.GradientAreaGapUtils;
import org.janelia.colormipsearch.cds.ShapeMatchScore;
import org.janelia.colormipsearch.cmd.cdsprocess.ColorMIPProcessUtils;
import org.janelia.colormipsearch.dao.DaosProvider;
import org.janelia.colormipsearch.dataio.CDMIPsWriter;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.dataio.NeuronMatchesWriter;
import org.janelia.colormipsearch.dataio.db.DBCheckedCDMIPsWriter;
import org.janelia.colormipsearch.dataio.db.DBNeuronMatchesReader;
import org.janelia.colormipsearch.dataio.db.DBNeuronMatchesWriter;
import org.janelia.colormipsearch.dataio.fs.JSONNeuronMatchesReader;
import org.janelia.colormipsearch.dataio.fs.JSONNeuronMatchesWriter;
import org.janelia.colormipsearch.datarequests.ScoresFilter;
import org.janelia.colormipsearch.datarequests.SortCriteria;
import org.janelia.colormipsearch.datarequests.SortDirection;
import org.janelia.colormipsearch.imageprocessing.ImageArray;
import org.janelia.colormipsearch.imageprocessing.ImageRegionDefinition;
import org.janelia.colormipsearch.mips.NeuronMIP;
import org.janelia.colormipsearch.mips.NeuronMIPUtils;
import org.janelia.colormipsearch.model.AbstractMatchEntity;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.CDMatchEntity;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.EMNeuronEntity;
import org.janelia.colormipsearch.model.FileData;
import org.janelia.colormipsearch.model.LMNeuronEntity;
import org.janelia.colormipsearch.model.ProcessingType;
import org.janelia.colormipsearch.results.GroupedMatchedEntities;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.janelia.colormipsearch.results.MatchEntitiesGrouping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command to calculate the gradient scores.
 */
class CalculateGradientScoresCmd extends AbstractCmd {

    private static final Logger LOG = LoggerFactory.getLogger(CalculateGradientScoresCmd.class);

    @Parameters(commandDescription = "Calculate gradient scores")
    static class CalculateGradientScoresArgs extends AbstractGradientScoresArgs {
        @Parameter(names = {"--nBestLines"},
                description = "Specifies the number of the top distinct lines to be used for gradient score")
        int numberOfBestLines;

        @Parameter(names = {"--nBestSamplesPerLine"},
                description = "Specifies the number of the top distinct samples within a line to be used for gradient score")
        int numberOfBestSamplesPerLine;

        @Parameter(names = {"--nBestMatchesPerSample"},
                description = "Number of best matches for each sample to be used for gradient scoring")
        int numberOfBestMatchesPerSample;


        CalculateGradientScoresArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }
    }

    private final CalculateGradientScoresArgs args;
    private final Supplier<Long> cacheSizeSupplier;
    private final ObjectMapper mapper;

    CalculateGradientScoresCmd(String commandName,
                               CommonArgs commonArgs,
                               Supplier<Long> cacheSizeSupplier) {
        super(commandName);
        this.args = new CalculateGradientScoresArgs(commonArgs);
        this.cacheSizeSupplier = cacheSizeSupplier;
        this.mapper = new ObjectMapper()
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        ;
    }

    @Override
    CalculateGradientScoresArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        // initialize the cache
        CachedMIPsUtils.initializeCache(cacheSizeSupplier.get());
        // run gradient scoring
        calculateAllGradientScores();
    }

    private void calculateAllGradientScores() {
        long startTime = System.currentTimeMillis();
        ImageRegionDefinition excludedRegions = args.getRegionGeneratorForTextLabels();
        ColorDepthSearchAlgorithmProvider<ShapeMatchScore> gradScoreAlgorithmProvider = ColorDepthSearchAlgorithmProviderFactory.createShapeMatchCDSAlgorithmProvider(
                args.mirrorMask,
                args.negativeRadius,
                args.borderSize,
                loadQueryROIMask(args.queryROIMaskName),
                excludedRegions
        );
        NeuronMatchesReader<CDMatchEntity<EMNeuronEntity, LMNeuronEntity>> cdMatchesReader = getCDMatchesReader();
        List<String> matchesMasksToProcess = cdMatchesReader.listMatchesLocations(
                args.masksLibraries.stream()
                        .map(larg -> new DataSourceParam()
                                .setAlignmentSpace(args.alignmentSpace)
                                .addLibrary(larg.input)
                                .addNames(args.masksPublishedNames)
                                .addMipIDs(args.masksMIPIDs)
                                .addDatasets(args.maskDatasets)
                                .addTags(args.maskTags)
                                .setOffset(larg.offset)
                                .setSize(larg.length))
                .collect(Collectors.toList()));
        int size = matchesMasksToProcess.size();
        Executor executor = CmdUtils.createCmdExecutor(args.commonArgs);
        // partition matches and process all partitions concurrently
        ItemsHandling.partitionCollection(matchesMasksToProcess, args.processingPartitionSize).entrySet().stream().parallel()
                .forEach(indexedPartition -> {
                    LOG.info("Start processing partition {} ({} items)",
                            indexedPartition.getKey(),
                            indexedPartition.getValue().size());
                    long startProcessingPartitionTime = System.currentTimeMillis();
                    // process each item from the current partition sequentially 
                    indexedPartition.getValue().forEach(maskIdToProcess -> {
                        // read all matches for the current mask
                        List<CDMatchEntity<EMNeuronEntity, LMNeuronEntity>> cdMatchesForMask = getCDMatchesForMask(cdMatchesReader, maskIdToProcess);
                        long nPublishedLines = cdMatchesForMask.stream()
                                .map(cdm -> cdm.getMatchedImage().getPublishedName())
                                .distinct()
                                .count();
                        // calculate the grad scores
                        LOG.info("Calculate grad scores for {} matches ({} lines) of {} - memory usage {}M out of {}M",
                                cdMatchesForMask.size(), nPublishedLines, maskIdToProcess,
                                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1, // round up
                                (Runtime.getRuntime().totalMemory() / _1M));
                        List<CDMatchEntity<EMNeuronEntity, LMNeuronEntity>> cdMatchesWithGradScores = calculateGradientScores(
                                gradScoreAlgorithmProvider,
                                cdMatchesForMask,
                                executor);
                        LOG.info("Completed grad scores for {} matches of {} - memory usage {}M out of {}M",
                                cdMatchesWithGradScores.size(), maskIdToProcess,
                                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1, // round up
                                (Runtime.getRuntime().totalMemory() / _1M));
                        long writtenUpdates = updateCDMatches(cdMatchesWithGradScores);
                        LOG.info("Updated {} grad scores for {} matches of {}", writtenUpdates, cdMatchesWithGradScores.size(), maskIdToProcess);
                        if (StringUtils.isNotBlank(args.processingTag)) {
                            long updatesWithProcessedTag = updateProcessingTag(cdMatchesForMask);
                            LOG.info("Set processing tag {} for {} mips", args.getProcessingTag(), updatesWithProcessedTag);
                        }
                        System.gc(); // explicitly garbage collect
                    });
                    LOG.info("Finished partition {} ({} items) in {}s - memory usage {}M out of {}M",
                            indexedPartition.getKey(),
                            indexedPartition.getValue().size(),
                            (System.currentTimeMillis() - startProcessingPartitionTime) / 1000.,
                            (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1, // round up
                            (Runtime.getRuntime().totalMemory() / _1M));
                });
        LOG.info("Finished calculating gradient scores for {} items in {}s - memory usage {}M out of {}M",
                size,
                (System.currentTimeMillis() - startTime) / 1000.,
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1, // round up
                (Runtime.getRuntime().totalMemory() / _1M));
    }

    /**
     * The ROI mask is typically the hemibrain mask that should be applied when the color depth search is done from LM to EM.
     *
     * @param queryROIMask the location of the ROI mask
     * @return
     */
    private ImageArray<?> loadQueryROIMask(String queryROIMask) {
        if (StringUtils.isBlank(queryROIMask)) {
            return null;
        } else {
            return NeuronMIPUtils.loadImageFromFileData(FileData.fromString(queryROIMask));
        }
    }

    private <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity> NeuronMatchesReader<CDMatchEntity<M, T>> getCDMatchesReader() {
        if (args.commonArgs.resultsStorage == StorageType.DB) {
            DaosProvider daosProvider = getDaosProvider();
            return new DBNeuronMatchesReader<>(
                    daosProvider.getNeuronMetadataDao(),
                    daosProvider.getCDMatchesDao(),
                    "mipId");
        } else {
            return new JSONNeuronMatchesReader<>(mapper);
        }
    }

    private <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity> NeuronMatchesWriter<CDMatchEntity<M, T>> getCDMatchesWriter() {
        if (args.commonArgs.resultsStorage == StorageType.DB) {
            return new DBNeuronMatchesWriter<>(getDaosProvider().getCDMatchesDao());
        } else {
            return new JSONNeuronMatchesWriter<>(
                    args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter(),
                    AbstractNeuronEntity::getMipId, // group results by neuron MIP ID
                    Comparator.comparingDouble(m -> -(((CDMatchEntity<?,?>) m).getNormalizedScore())), // descending order by matching pixels
                    args.getOutputDir(),
                    null
            );
        }
    }

    private Optional<CDMIPsWriter> getCDMipsWriter() {
        if (args.commonArgs.resultsStorage == StorageType.DB) {
            return Optional.of(new DBCheckedCDMIPsWriter(getDaosProvider().getNeuronMetadataDao()));
        } else {
            return Optional.empty();
        }
    }

    /**
     * The method calculates and updates the gradient scores for all color depth matches of the given mask MIP ID.
     *
     * @param gradScoreAlgorithmProvider grad score algorithm provider
     * @param cdMatches                  color depth matches for which the grad score will be computed
     * @param executor                   task executor
     * @param <M>                        mask type
     * @param <T>                        target type
     */
    @SuppressWarnings("unchecked")
    private <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity>
    List<CDMatchEntity<M, T>> calculateGradientScores(
            ColorDepthSearchAlgorithmProvider<ShapeMatchScore> gradScoreAlgorithmProvider,
            List<CDMatchEntity<M, T>> cdMatches,
            Executor executor) {
        // group the matches by the mask input file - this is because we do not want to mix FL and non-FL neuron images for example
        List<GroupedMatchedEntities<M, T, CDMatchEntity<M, T>>> selectedMatchesGroupedByInput =
                MatchEntitiesGrouping.simpleGroupByMaskFields(
                        cdMatches,
                        Arrays.asList(
                                AbstractNeuronEntity::getMipId,
                                m -> m.getComputeFileName(ComputeFileType.InputColorDepthImage)
                        )
                );
        List<CompletableFuture<CDMatchEntity<M, T>>> gradScoreComputations = selectedMatchesGroupedByInput.stream()
                .flatMap(selectedMaskMatches -> runGradScoreComputations(
                        selectedMaskMatches.getKey(),
                        selectedMaskMatches.getItems(),
                        gradScoreAlgorithmProvider,
                        executor
                ).stream())
                .collect(Collectors.toList());
        // wait for all computation to finish
        List<CDMatchEntity<M, T>> matchesWithGradScores = gradScoreComputations.stream()
                .map(CompletableFuture::join)
                .filter(CDMatchEntity::hasGradScore)
                .collect(Collectors.toList());

        updateNormalizedScores(matchesWithGradScores);

        return matchesWithGradScores;
    }

    private <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity> long updateCDMatches(List<CDMatchEntity<M, T>> cdMatches) {
        NeuronMatchesWriter<CDMatchEntity<M, T>> matchesWriter = getCDMatchesWriter();
        return matchesWriter.writeUpdates(
                cdMatches,
                Arrays.asList(
                        m -> ImmutablePair.of("gradientAreaGap", m.getGradientAreaGap()),
                        m -> ImmutablePair.of("highExpressionArea", m.getHighExpressionArea()),
                        m -> ImmutablePair.of("normalizedScore", m.getNormalizedScore()),
                        m -> ImmutablePair.of("updatedDate", new Date())
                ));
    }

    private <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity> long updateProcessingTag(List<CDMatchEntity<M, T>> cdMatches) {
        Set<String> processingTags = Collections.singleton(args.getProcessingTag());
        return getCDMipsWriter()
                .map(cdmipsWriter -> {
                    Set<M> masksToUpdate = cdMatches.stream()
                            .map(AbstractMatchEntity::getMaskImage).collect(Collectors.toSet());
                    Set<T> targetsToUpdate = cdMatches.stream()
                            .map(AbstractMatchEntity::getMatchedImage).collect(Collectors.toSet());
                    cdmipsWriter.addProcessingTags(masksToUpdate, ProcessingType.GradientScore, processingTags);
                    cdmipsWriter.addProcessingTags(targetsToUpdate, ProcessingType.GradientScore, processingTags);
                    return masksToUpdate.size() + targetsToUpdate.size();
                })
                .orElse(0);
    }

    private <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity>
    List<CDMatchEntity<M, T>> getCDMatchesForMask(NeuronMatchesReader<CDMatchEntity<M, T>> cdsMatchesReader, String maskCDMipId) {
        LOG.info("Read all color depth matches for {}", maskCDMipId);
        ScoresFilter neuronsMatchScoresFilter = new ScoresFilter();
        if (args.pctPositivePixels > 0) {
            neuronsMatchScoresFilter.addSScore("matchingPixelsRatio", args.pctPositivePixels / 100);
        }
        List<CDMatchEntity<M, T>> allCDMatches = cdsMatchesReader.readMatchesByMask(
                args.alignmentSpace,
                /* maskLibraries */null,
                /* maskPublishedNames */null,
                Collections.singletonList(maskCDMipId),
                args.maskDatasets,
                args.maskTags,
                /*maskExcludedTags*/null,
                args.targetsLibraries,
                args.targetsPublishedNames,
                args.targetsMIPIDs,
                args.targetDatasets,
                args.targetTags,
                /*targetExcludedTags*/null,
                args.matchTags,
                /*matchExcludedTags*/null,
                neuronsMatchScoresFilter,
                Collections.singletonList(
                        new SortCriteria("normalizedScore", SortDirection.DESC)
                ));
        // select best matches to process
        LOG.info("Select best color depth matches for {} out of {} total matches", maskCDMipId, allCDMatches.size());
        return ColorMIPProcessUtils.selectBestMatches(
                allCDMatches,
                args.numberOfBestLines,
                args.numberOfBestSamplesPerLine,
                args.numberOfBestMatchesPerSample
        );
    }

    private <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity>
    List<CompletableFuture<CDMatchEntity<M, T>>> runGradScoreComputations(M mask,
                                                                          List<CDMatchEntity<M, T>> selectedMatches,
                                                                          ColorDepthSearchAlgorithmProvider<ShapeMatchScore> gradScoreAlgorithmProvider,
                                                                          Executor executor) {
        LOG.info("Prepare gradient score computations for {} with {} matches", mask, selectedMatches.size());
        LOG.info("Load query image {}", mask);
        NeuronMIP<M> maskImage = NeuronMIPUtils.loadComputeFile(mask, ComputeFileType.InputColorDepthImage);
        if (NeuronMIPUtils.hasNoImageArray(maskImage) || CollectionUtils.isEmpty(selectedMatches)) {
            LOG.error("No image found for {}", mask);
            return Collections.emptyList();
        }
        ColorDepthSearchAlgorithm<ShapeMatchScore> gradScoreAlgorithm =
                gradScoreAlgorithmProvider.createColorDepthQuerySearchAlgorithmWithDefaultParams(
                        maskImage.getImageArray(),
                        args.maskThreshold,
                        args.borderSize);
        Set<ComputeFileType> requiredVariantTypes = gradScoreAlgorithm.getRequiredTargetVariantTypes();
        return selectedMatches.stream()
                .map(cdsMatch -> CompletableFuture.supplyAsync(() -> {
                            long startCalcTime = System.currentTimeMillis();
                            T matchedTarget = cdsMatch.getMatchedImage();
                            NeuronMIP<T> matchedTargetImage = CachedMIPsUtils.loadMIP(matchedTarget, ComputeFileType.InputColorDepthImage);
                            if (NeuronMIPUtils.hasImageArray(matchedTargetImage)) {
                                LOG.debug("Calculate grad score between {} and {}",
                                        cdsMatch.getMaskImage(), cdsMatch.getMatchedImage());
                                ShapeMatchScore gradScore = gradScoreAlgorithm.calculateMatchingScore(
                                        matchedTargetImage.getImageArray(),
                                        NeuronMIPUtils.getImageLoaders(
                                                matchedTarget,
                                                requiredVariantTypes,
                                                (n, cft) -> NeuronMIPUtils.getImageArray(CachedMIPsUtils.loadMIP(n, cft))
                                        )
                                );
                                cdsMatch.setGradientAreaGap(gradScore.getGradientAreaGap());
                                cdsMatch.setHighExpressionArea(gradScore.getHighExpressionArea());
                                cdsMatch.setNormalizedScore(gradScore.getNormalizedScore());
                                LOG.debug("Finished calculating negative score between {} and {} in {}ms",
                                        cdsMatch.getMaskImage(), cdsMatch.getMatchedImage(), System.currentTimeMillis() - startCalcTime);
                            } else {
                                cdsMatch.setGradientAreaGap(-1L);
                            }
                            return cdsMatch;
                        },
                        executor))
                .collect(Collectors.toList());
    }

    private <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity> void updateNormalizedScores(List<CDMatchEntity<M, T>> cdMatches) {
        // get max scores for normalization
        CombinedMatchScore maxScores = cdMatches.stream()
                .map(m -> new CombinedMatchScore(m.getMatchingPixels(), m.getGradScore()))
                .reduce(new CombinedMatchScore(-1, -1L),
                        (s1, s2) -> new CombinedMatchScore(
                                Math.max(s1.getPixelMatches(), s2.getPixelMatches()),
                                Math.max(s1.getGradScore(), s2.getGradScore())));
        // update normalized score
        cdMatches.forEach(m -> m.setNormalizedScore((float) GradientAreaGapUtils.calculateNormalizedScore(
                m.getMatchingPixels(),
                m.getGradientAreaGap(),
                m.getHighExpressionArea(),
                maxScores.getPixelMatches(),
                maxScores.getGradScore()
        )));
    }

}
