package org.janelia.colormipsearch.ppp;

import java.util.Comparator;
import java.util.List;

import org.janelia.colormipsearch.results.ItemsHandling;
import org.janelia.colormipsearch.results.GroupingCriteria;
import org.janelia.colormipsearch.results.ResultMatches;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.EMNeuronMetadata;
import org.janelia.colormipsearch.model.LMNeuronMetadata;
import org.janelia.colormipsearch.model.PPPMatch;

public class PPPGrouping {
    /**
     * Group PPP matches by neuron body ID.
     *
     * @param pppMatches
     * @return a list of grouped PPP matches by neuron body ID
     */
    public static List<ResultMatches<EMNeuronMetadata, LMNeuronMetadata, PPPMatch<EMNeuronMetadata, LMNeuronMetadata>>> groupByNeuronBodyId(List<PPPMatch<EMNeuronMetadata, LMNeuronMetadata>> pppMatches) {
        return ItemsHandling.groupItems(
                pppMatches,
                aPPPMatch -> new GroupingCriteria<>(
                        aPPPMatch,
                        AbstractMatch::getMaskImage,
                        (k1, k2) -> k1.getPublishedName().equals(k2.getPublishedName()),
                        AbstractNeuronMetadata::hashCode
                ),
                g -> {
                    PPPMatch<EMNeuronMetadata, LMNeuronMetadata> aPPPMatch = g.getItem();
                    aPPPMatch.resetMaskImage();
                    return aPPPMatch;
                },
                Comparator.comparingDouble(aPPPMatch -> Math.abs(aPPPMatch.getRank())),
                ResultMatches::new
        );
    }
}
