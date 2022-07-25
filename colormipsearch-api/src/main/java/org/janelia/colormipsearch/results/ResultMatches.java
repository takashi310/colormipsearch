package org.janelia.colormipsearch.results;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.JsonRequired;

public class ResultMatches<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>> extends AbstractGroupedItems<R, M> {

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
