package org.janelia.colormipsearch.cmd.cdsprocess;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.google.common.collect.Streams;

import org.janelia.colormipsearch.cds.PixelMatchScore;
import org.janelia.colormipsearch.cds.ColorDepthSearchAlgorithm;
import org.janelia.colormipsearch.cds.ColorMIPSearch;
import org.janelia.colormipsearch.cmd.CachedMIPsUtils;
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
public class LocalColorMIPSearchProcessor<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> extends AbstractColorMIPSearchProcessor<M, T> {

    private static final Logger LOG = LoggerFactory.getLogger(LocalColorMIPSearchProcessor.class);
    private static final long _1M = 1024 * 1024;

    private final Executor cdsExecutor;

    public LocalColorMIPSearchProcessor(ColorMIPSearch colorMIPSearch,
                                        int localProcessingPartitionSize,
                                        Executor cdsExecutor) {
        super(colorMIPSearch, localProcessingPartitionSize);
        this.cdsExecutor = cdsExecutor;
    }

    @Override
    public List<CDSMatch<M, T>> findAllColorDepthMatches(List<M> queryMIPs, List<T> targetMIPs) {
        long startTime = System.currentTimeMillis();
        int nQueries = queryMIPs.size();
        int nTargets = targetMIPs.size();

        LOG.info("Searching {} masks against {} targets", nQueries, nTargets);

        List<CompletableFuture<List<CDSMatch<M, T>>>> allColorDepthSearches = Streams.zip(
                LongStream.range(0, queryMIPs.size()).boxed(),
                queryMIPs.stream(),
                (mIndex, maskMIP) -> submitMaskSearches(mIndex + 1, maskMIP, targetMIPs))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        LOG.info("Submitted all {} color depth searches for {} masks with {} targets in {}s - memory usage {}M",
                allColorDepthSearches.size(), queryMIPs.size(), targetMIPs.size(),
                (System.currentTimeMillis() - startTime) / 1000.,
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1);

        List<CDSMatch<M, T>> allSearchResults = CompletableFuture.allOf(allColorDepthSearches.toArray(new CompletableFuture<?>[0]))
                .thenApply(ignoredVoidResult -> allColorDepthSearches.stream()
                        .flatMap(searchComputation -> searchComputation.join().stream())
                        .collect(Collectors.toList()))
                .join();

        LOG.info("Finished all color depth searches {} masks with {} targets in {}s - memory usage {}M",
                queryMIPs.size(), targetMIPs.size(), (System.currentTimeMillis() - startTime) / 1000.,
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1);
        return allSearchResults;
    }

    private List<CompletableFuture<List<CDSMatch<M, T>>>> submitMaskSearches(long mIndex,
                                                                             M queryMIP,
                                                                             List<T> targetMIPs) {
        NeuronMIP<M> queryImage = NeuronMIPUtils.loadComputeFile(queryMIP, ComputeFileType.InputColorDepthImage); // load image - no caching for the mask
        if (queryImage == null || queryImage.hasNoImageArray()) {
            return Collections.singletonList(
                    CompletableFuture.completedFuture(Collections.emptyList())
            );
        }
        ColorDepthSearchAlgorithm<PixelMatchScore> queryColorDepthSearch = colorMIPSearch.createQueryColorDepthSearchWithDefaultThreshold(queryImage.getImageArray());
        if (queryColorDepthSearch.getQuerySize() == 0) {
            LOG.info("No computation created for {} because it is empty", queryMIP);
            return Collections.emptyList();
        }
        List<CompletableFuture<List<CDSMatch<M, T>>>> cdsComputations = ItemsHandling.partitionCollection(targetMIPs, localProcessingPartitionSize).stream()
                .map(targetMIPsPartition -> {
                    Supplier<List<CDSMatch<M, T>>> searchResultSupplier = () -> {
                        LOG.debug("Compare query# {} - {} with {} out of {} targets", mIndex, queryMIP, targetMIPsPartition.size(), targetMIPs.size());
                        long startTime = System.currentTimeMillis();
                        List<CDSMatch<M, T>> srs = targetMIPsPartition.stream()
                                .map(targetMIP -> CachedMIPsUtils.loadMIP(targetMIP, ComputeFileType.InputColorDepthImage))
                                .filter(NeuronMIPUtils::hasImageArray)
                                .map(targetImage -> findPixelMatch(queryColorDepthSearch, queryImage, targetImage))
                                .filter(m -> m.isMatchFound() && m.hasNoErrors())
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
