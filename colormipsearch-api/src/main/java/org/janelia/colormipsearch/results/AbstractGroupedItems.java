package org.janelia.colormipsearch.results;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * @param <K> key type
 * @param <T> type of grouped elements
 */
public abstract class AbstractGroupedItems<K, T> {

    private K key;
    private List<T> items;

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public List<T> getItems() {
        return items;
    }

    public void setItems(List<T> items) {
        this.items = items;
    }

    @JsonIgnore
    public int getItemsCount() {
        return items == null ? 0 : items.size();
    }
}
