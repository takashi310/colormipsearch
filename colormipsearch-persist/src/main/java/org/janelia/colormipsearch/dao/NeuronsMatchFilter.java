package org.janelia.colormipsearch.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.datarequests.ScoresFilter;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public class NeuronsMatchFilter<R extends AbstractMatch<? extends AbstractNeuronMetadata, ? extends AbstractNeuronMetadata>> {

    private ScoresFilter scoresFilter;
    private List<Number> maskEntityIds;
    private List<Number> targetEntityIds;

    public ScoresFilter getScoresFilter() {
        return scoresFilter;
    }

    public NeuronsMatchFilter<R> setScoresFilter(ScoresFilter scoresFilter) {
        this.scoresFilter = scoresFilter;
        return this;
    }

    public List<Number> getMaskEntityIds() {
        return maskEntityIds;
    }

    public NeuronsMatchFilter<R> setMaskEntityIds(List<Number> maskEntityIds) {
        this.maskEntityIds = maskEntityIds;
        return this;
    }

    public List<Number> getTargetEntityIds() {
        return targetEntityIds;
    }

    public NeuronsMatchFilter<R> setTargetEntityIds(List<Number> targetEntityIds) {
        this.targetEntityIds = targetEntityIds;
        return this;
    }

    public boolean isEmpty() {
        return (scoresFilter == null || scoresFilter.isEmpty())
            && CollectionUtils.isEmpty(maskEntityIds)
            && CollectionUtils.isEmpty(targetEntityIds);
    }

}
