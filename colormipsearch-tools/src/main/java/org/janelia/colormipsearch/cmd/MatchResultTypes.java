package org.janelia.colormipsearch.cmd;

import java.util.Comparator;
import java.util.function.Function;

import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.model.PPPMatch;

public enum MatchResultTypes {
    // color depth search matches
    CDS(CDMatch.class,
            AbstractNeuronMetadata::getMipId, // grouped by MIP ID
            Comparator.comparingDouble(m -> -(((CDMatch<?,?>) m).getNormalizedScore()))), // sorted by normalized score descending
    // PPP matches
    PPP(PPPMatch.class,
            AbstractNeuronMetadata::getPublishedName, // grouped by neuron name
            Comparator.comparingDouble(m -> (((PPPMatch<?,?>) m).getRank()))); // sorted by rank

    private Class<? extends AbstractMatch> matchType;
    private Function<AbstractNeuronMetadata, String> matchGrouping;
    private Comparator<AbstractMatch<?, ?>> matchOrdering;

    MatchResultTypes(Class<? extends AbstractMatch> matchType,
                     Function<AbstractNeuronMetadata, String> matchGrouping,
                     Comparator<AbstractMatch<?, ?>> matchOrdering) {
        this.matchType = matchType;
        this.matchGrouping = matchGrouping;
        this.matchOrdering = matchOrdering;
    }

    public String getMatchType() {
        return matchType.getName();
    }

    public Function<AbstractNeuronMetadata, String> getMatchGrouping() {
        return matchGrouping;
    }

    public Comparator<AbstractMatch<?, ?>> getMatchOrdering() {
        return matchOrdering;
    }
}
