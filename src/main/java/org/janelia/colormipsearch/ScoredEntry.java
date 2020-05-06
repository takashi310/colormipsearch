package org.janelia.colormipsearch;

class ScoredEntry<E> {
    String name;
    Number score;
    E entry;

    ScoredEntry(String name, Number score, E entry) {
        this.name = name;
        this.score = score;
        this.entry = entry;
    }
}
