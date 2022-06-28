package org.janelia.colormipsearch.results;

import java.util.Comparator;
import java.util.List;

import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public class MatchResultsGrouping {
    /**
     * Group matches by mask.
     *
     * @param matches
     * @return a list of grouped PPP matches by neuron body ID
     */
    public static <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>> List<ResultMatches<M, T, R>> groupByMask(List<R> matches,
                                                                                                                                                               Comparator<R> ranking) {
        return ItemsHandling.groupItems(
                matches,
                aMatch -> new GroupingCriteria<>(
                        aMatch,
                        AbstractMatch::getMaskImage,
                        (k1, k2) -> k1.getPublishedName().equals(k2.getPublishedName()),
                        AbstractNeuronMetadata::hashCode
                ),
                g -> {
                    R aMatch = g.getItem();
                    aMatch.resetMaskImage();
                    return aMatch;
                },
                ranking,
                ResultMatches::new
        );
    }

    @SuppressWarnings("unchecked")
    public static <M extends AbstractNeuronMetadata,
            T extends AbstractNeuronMetadata,
            R extends AbstractMatch<M, T>,
            R1 extends AbstractMatch<T, M>> List<ResultMatches<T, M, R1>> groupByMatchedImage(List<R> matches,
                                                                                              Comparator<R1> ranking) {
        return ItemsHandling.groupItems(
                matches,
                aMatch -> new GroupingCriteria<R1, T>(
                        (R1) aMatch.duplicate((src, dest) -> {
                            dest.setMaskImage(src.getMatchedImage());
                            dest.setMatchedImage(src.getMaskImage());
                        }),
                        AbstractMatch::getMaskImage,
                        (k1, k2) -> k1.getPublishedName().equals(k2.getPublishedName()),
                        AbstractNeuronMetadata::hashCode
                ),
                g -> {
                    R1 aMatch = g.getItem();
                    aMatch.resetMaskImage();
                    return aMatch;
                },
                ranking,
                ResultMatches::new
        );
    }

}
