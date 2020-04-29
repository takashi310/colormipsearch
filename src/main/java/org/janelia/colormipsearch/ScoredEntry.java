package org.janelia.colormipsearch;

class ScoredEntry<E> {
    String name;
    Double score;
    E entry;

    ScoredEntry(String name, Double score, E entry) {
        this.name = name;
        this.score = score;
        this.entry = entry;
    }
}
