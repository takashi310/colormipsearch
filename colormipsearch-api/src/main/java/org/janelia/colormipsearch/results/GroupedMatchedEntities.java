package org.janelia.colormipsearch.results;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.janelia.colormipsearch.model.AbstractMatchEntity;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.JsonRequired;

public class GroupedMatchedEntities<M extends AbstractNeuronEntity, T extends AbstractNeuronEntity, R extends AbstractMatchEntity<M, T>> extends GroupedItems<M, R> {

    @JsonRequired
    @JsonProperty("inputImage")
    @Override
    public M getKey() {
        return super.getKey();
    }

    @Override
    public void setKey(M key) {
        super.setKey(key);
    }

    @JsonRequired
    @JsonProperty("results")
    @Override
    public List<R> getItems() {
        return super.getItems();
    }

    @Override
    public void setItems(List<R> items) {
        super.setItems(items);
    }
}
