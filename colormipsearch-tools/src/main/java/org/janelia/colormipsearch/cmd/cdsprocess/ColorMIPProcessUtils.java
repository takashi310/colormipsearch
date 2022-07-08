package org.janelia.colormipsearch.cmd.cdsprocess;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.cmd.CachedMIPsUtils;
import org.janelia.colormipsearch.imageprocessing.ImageArray;
import org.janelia.colormipsearch.mips.NeuronMIPUtils;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDSMatch;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.EMNeuronMetadata;
import org.janelia.colormipsearch.model.LMNeuronMetadata;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.janelia.colormipsearch.results.ScoredEntry;

public class ColorMIPProcessUtils {
    public static <N extends AbstractNeuronMetadata> Map<ComputeFileType, Supplier<ImageArray<?>>> getVariantImagesSuppliers(Set<ComputeFileType> variantTypes,
                                                                                                                             N neuronMIP) {
        return variantTypes.stream()
                .map(cft -> {
                    Pair<ComputeFileType, Supplier<ImageArray<?>>> e =
                            ImmutablePair.of(
                                    cft,
                                    () -> NeuronMIPUtils.getImageArray(CachedMIPsUtils.loadMIP(neuronMIP, cft))
                            );
                    return e;
                })
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight))
                ;
    }

    public static <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> List<CDSMatch<M, T>> selectBestMatches(List<CDSMatch<M, T>> cdsMatches,
                                                                                                                              int topLineMatches,
                                                                                                                              int topSamplesPerLine,
                                                                                                                              int topMatchesPerSample) {
        List<ScoredEntry<List<CDSMatch<M, T>>>> topRankedLineMatches = ItemsHandling.selectTopRankedElements(
                cdsMatches,
                match -> match.getMatchedImage().getPublishedName(),
                CDSMatch::getMatchingPixels,
                topLineMatches,
                -1);

        return topRankedLineMatches.stream()
                .flatMap(se -> ItemsHandling.selectTopRankedElements( // topRankedSamplesPerLine
                        se.getEntry(),
                        match -> match.getMatchedImage().getNeuronId(),
                        CDSMatch::getMatchingPixels,
                        topSamplesPerLine,
                        topMatchesPerSample
                ).stream())
                .flatMap(se -> se.getEntry().stream())
                .collect(Collectors.toList())
                ;
    }
}
