package org.janelia.colormipsearch.results;

import java.util.Comparator;
import java.util.List;

import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public class PPPGrouping {
    /**
     * Group PPP matches by neuron body ID.
     *
     * @param pppMatches
     * @return a list of grouped PPP matches by neuron body ID
     */
    public static <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>> List<ResultMatches<M, T, R>> groupByNeuronBodyId(List<R> pppMatches,
                                                                                                                                                                       Comparator<R> ranking) {
        return ItemsHandling.groupItems(
                pppMatches,
                aPPPMatch -> new GroupingCriteria<>(
                        aPPPMatch,
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
}
