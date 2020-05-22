package org.janelia.colormipsearch.tools;

public class ScoredEntry<E> {
    private final String name;
    private final Number score;
    private final E entry;

    ScoredEntry(String name, Number score, E entry) {
        this.name = name;
        this.score = score;
        this.entry = entry;
    }

    public String getName() {
        return name;
    }

    public Number getScore() {
        return score;
    }

    public E getEntry() {
        return entry;
    }
}
