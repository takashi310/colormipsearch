package org.janelia.colormipsearch.dao;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.datarequests.ScoresFilter;
import org.janelia.colormipsearch.model.AbstractMatchEntity;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;

public class NeuronsMatchFilter<R extends AbstractMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> {

    private ScoresFilter scoresFilter; // score filter
    private List<Number> maskEntityIds; // mask entity IDs
    private List<Number> targetEntityIds; // target entity IDs
    private final Set<String> tags = new HashSet<>(); // matching tags

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

    public Set<String> getTags() {
        return tags;
    }

    public NeuronsMatchFilter<R> addTag(String tag) {
        if (StringUtils.isNotBlank(tag)) this.tags.add(tag);
        return this;
    }

    public NeuronsMatchFilter<R> addTags(Collection<String> tags) {
        if (tags != null) tags.forEach(this::addTag);
        return this;
    }

    public boolean hasTags() {
        return CollectionUtils.isNotEmpty(tags);
    }

    public boolean isEmpty() {
        return (scoresFilter == null || scoresFilter.isEmpty())
                && CollectionUtils.isEmpty(maskEntityIds)
                && CollectionUtils.isEmpty(targetEntityIds)
                && !hasTags();
    }
}
