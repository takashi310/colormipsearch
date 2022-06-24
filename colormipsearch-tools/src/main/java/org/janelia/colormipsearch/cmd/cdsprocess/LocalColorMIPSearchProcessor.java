package org.janelia.colormipsearch.cmd.cdsprocess;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.google.common.collect.Streams;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.cds.ColorDepthSearchAlgorithm;
import org.janelia.colormipsearch.cds.ColorMIPMatchScore;
import org.janelia.colormipsearch.cds.ColorMIPSearch;
import org.janelia.colormipsearch.cmd.CachedMIPsUtils;
import org.janelia.colormipsearch.imageprocessing.ImageArray;
import org.janelia.colormipsearch.imageprocessing.MappingFunction;
import org.janelia.colormipsearch.mips.NeuronMIP;
import org.janelia.colormipsearch.mips.NeuronMIPUtils;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDSMatch;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform color depth mask search in the current process.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class LocalColorMIPSearchProcessor<M extends AbstractNeuronMetadata, I extends AbstractNeuronMetadata> implements ColorMIPSearchProcessor<M, I> {

    private static final Logger LOG = LoggerFactory.getLogger(LocalColorMIPSearchProcessor.class);
    private static final long _1M = 1024 * 1024;

    private final ColorMIPSearch colorMIPSearch;
    private final Executor cdsExecutor;
    private final int localProcessingPartitionSize;
    private final List<String> gradientsLocations;
    private final MappingFunction<String, String> gradientVariantSuffixMapping;
    private final List<String> zgapMasksLocations;
    private final MappingFunction<String, String> zgapMaskVariantSuffixMapping;

    public LocalColorMIPSearchProcessor(ColorMIPSearch colorMIPSearch,
                                        int localProcessingPartitionSize,
                                        List<String> gradientsLocations,
                                        MappingFunction<String, String> gradientVariantSuffixMapping,
                                        List<String> zgapMasksLocations,
                                        MappingFunction<String, String> zgapMaskVariantSuffixMapping,
                                        Executor cdsExecutor) {
        this.colorMIPSearch = colorMIPSearch;
        this.localProcessingPartitionSize = localProcessingPartitionSize > 0 ? localProcessingPartitionSize : 1;
        this.gradientsLocations = gradientsLocations;
        this.gradientVariantSuffixMapping = gradientVariantSuffixMapping;
        this.zgapMasksLocations = zgapMasksLocations;
        this.zgapMaskVariantSuffixMapping = zgapMaskVariantSuffixMapping;
        this.cdsExecutor = cdsExecutor;
    }

    @Override
    public List<CDSMatch<M, I>> findAllColorDepthMatches(List<M> queryMIPs, List<I> targetMIPs) {
        long startTime = System.currentTimeMillis();
        int nQueries = queryMIPs.size();
        int nTargets = targetMIPs.size();

        LOG.info("Searching {} masks against {} targets", nQueries, nTargets);

        List<CompletableFuture<List<CDSMatch<M, I>>>> allColorDepthSearches = Streams.zip(
                LongStream.range(0, queryMIPs.size()).boxed(),
                queryMIPs.stream(),
                (mIndex, maskMIP) -> submitMaskSearches(mIndex + 1, maskMIP, targetMIPs))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        LOG.info("Submitted all {} color depth searches for {} masks with {} targets in {}s - memory usage {}M",
                allColorDepthSearches.size(), queryMIPs.size(), targetMIPs.size(),
                (System.currentTimeMillis() - startTime) / 1000.,
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1);

        List<CDSMatch<M, I>> allSearchResults = CompletableFuture.allOf(allColorDepthSearches.toArray(new CompletableFuture<?>[0]))
                .thenApply(ignoredVoidResult -> allColorDepthSearches.stream()
                        .flatMap(searchComputation -> searchComputation.join().stream())
                        .collect(Collectors.toList()))
                .join();

        LOG.info("Finished all color depth searches {} masks with {} targets in {}s - memory usage {}M",
                queryMIPs.size(), targetMIPs.size(), (System.currentTimeMillis() - startTime) / 1000.,
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1);
        return allSearchResults;
    }

    private List<CompletableFuture<List<CDSMatch<M, I>>>> submitMaskSearches(long mIndex,
                                                                             M queryMIP,
                                                                             List<I> targetMIPs) {
        NeuronMIP<M> queryImage = NeuronMIPUtils.loadComputeFile(queryMIP, ComputeFileType.InputColorDepthImage); // load image - no caching for the mask
        if (queryImage == null || queryImage.hasNoImageArray()) {
            return Collections.singletonList(
                    CompletableFuture.completedFuture(Collections.emptyList())
            );
        }
        ColorDepthSearchAlgorithm<ColorMIPMatchScore> queryColorDepthSearch = colorMIPSearch.createQueryColorDepthSearchWithDefaultThreshold(queryImage);
        if (queryColorDepthSearch.getQuerySize() == 0) {
            LOG.info("No computation created for {} because it is empty", queryMIP);
            return Collections.emptyList();
        }
        Set<ComputeFileType> requiredVariantTypes = queryColorDepthSearch.getRequiredTargetVariantTypes();
        List<CompletableFuture<List<CDSMatch<M, I>>>> cdsComputations = ItemsHandling.partitionCollection(targetMIPs, localProcessingPartitionSize).stream()
                .map(targetMIPsPartition -> {
                    Supplier<List<CDSMatch<M, I>>> searchResultSupplier = () -> {
                        LOG.debug("Compare query# {} - {} with {} out of {} targets", mIndex, queryMIP, targetMIPsPartition.size(), targetMIPs.size());
                        long startTime = System.currentTimeMillis();
                        List<CDSMatch<M, I>> srs = targetMIPsPartition.stream()
                                .map(targetMIP -> CachedMIPsUtils.loadMIP(targetMIP, ComputeFileType.InputColorDepthImage))
                                .filter(mip -> mip != null)
                                .map(targetImage -> {
                                    try {
                                        Map<ComputeFileType, Supplier<ImageArray<?>>> variantImageSuppliers =
                                                requiredVariantTypes.stream()
                                                        .map(cft -> {
                                                            Pair<ComputeFileType, Supplier<ImageArray<?>>> e =
                                                                    ImmutablePair.of(
                                                                            cft,
                                                                            () -> NeuronMIPUtils.getImageArray(CachedMIPsUtils.loadMIP(targetImage.getNeuronInfo(), cft))
                                                                    );
                                                            return e;
                                                        })
                                                        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight))
                                                ;
                                        ColorMIPMatchScore colorMIPMatchScore = queryColorDepthSearch.calculateMatchingScore(
                                                NeuronMIPUtils.getImageArray(targetImage),
                                                variantImageSuppliers);
                                        boolean isMatch = colorMIPSearch.isMatch(colorMIPMatchScore);
                                        if (isMatch) {
                                            CDSMatch<M, I> match = new CDSMatch<>();
                                            match.setMaskImage(queryImage.getNeuronInfo());
                                            match.setMatchedImage(targetImage.getNeuronInfo());
                                            match.setMatchingPixels(colorMIPMatchScore.getMatchingPixNum());
                                            match.setMirrored(colorMIPMatchScore.isMirrored());
                                            // !!!! FIXME - set score
                                            return match;
                                        } else {
                                            return null;
                                        }
                                    } catch (Throwable e) {
                                        LOG.warn("Error comparing mask {} with {}", queryMIP,  targetImage, e);
                                        CDSMatch<M, I> match = new CDSMatch<>();
                                        match.setMaskImage(queryImage.getNeuronInfo());
                                        match.setMatchedImage(targetImage.getNeuronInfo());
                                        match.setErrors(e.getMessage());
                                        return match;
                                    }
                                })
                                .filter(m -> m != null && !m.hasErrors())
                                .collect(Collectors.toList());
                        LOG.info("Found {} matches comparing mask# {} - {} with {} out of {} libraries in {}ms",
                                srs.size(), mIndex, queryMIP, targetMIPsPartition.size(), targetMIPs.size(), System.currentTimeMillis() - startTime);
                        return srs;
                    };
                    return CompletableFuture.supplyAsync(searchResultSupplier, cdsExecutor);
                })
                .collect(Collectors.toList());
        LOG.info("Submitted {} partitioned color depth searches with {} libraries for mask# {} - {}",
                cdsComputations.size(), targetMIPs.size(), mIndex, queryMIP);
        return cdsComputations;
    }

    @Override
    public void terminate() {
        // nothing to do here
    }
}
