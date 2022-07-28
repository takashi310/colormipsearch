package org.janelia.colormipsearch.cmd;

import java.util.Comparator;
import java.util.function.Function;

import org.janelia.colormipsearch.model.AbstractMatchEntity;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.CDMatchEntity;
import org.janelia.colormipsearch.model.PPPMatchEntity;

public enum MatchResultTypes {
    // color depth search matches
    CDS(CDMatchEntity.class,
            AbstractNeuronEntity::getMipId, // grouped by MIP ID
            Comparator.comparingDouble(m -> -(((CDMatchEntity<?,?>) m).getNormalizedScore()))), // sorted by normalized score descending
    // PPP matches
    PPP(PPPMatchEntity.class,
            AbstractNeuronEntity::getPublishedName, // grouped by published name
            Comparator.comparingDouble(m -> (((PPPMatchEntity<?,?>) m).getRank()))); // sorted by rank

    @SuppressWarnings("rawtypes")
    private final Class<? extends AbstractMatchEntity> matchType;
    private final Function<AbstractNeuronEntity, String> matchGrouping;
    private final Comparator<AbstractMatchEntity<?, ?>> matchOrdering;

    @SuppressWarnings("rawtypes")
    MatchResultTypes(Class<? extends AbstractMatchEntity> matchType,
                     Function<AbstractNeuronEntity, String> matchGrouping,
                     Comparator<AbstractMatchEntity<?, ?>> matchOrdering) {
        this.matchType = matchType;
        this.matchGrouping = matchGrouping;
        this.matchOrdering = matchOrdering;
    }

    public String getMatchType() {
        return matchType.getName();
    }

    public Function<AbstractNeuronEntity, String> getMatchGrouping() {
        return matchGrouping;
    }

    public Comparator<AbstractMatchEntity<?, ?>> getMatchOrdering() {
        return matchOrdering;
    }
}
