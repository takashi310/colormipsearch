package org.janelia.colormipsearch.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public class NeuronsMatchFilter<R extends AbstractMatch<? extends AbstractNeuronMetadata, ? extends AbstractNeuronMetadata>> {
    public static class ScoreField {
        private final String fieldName;
        private final Double minScore;

        private ScoreField(String fieldName, Double minScore) {
            this.fieldName = fieldName;
            this.minScore = minScore;
        }

        public String getFieldName() {
            return fieldName;
        }

        public Double getMinScore() {
            return minScore;
        }
    }

    private String matchType;
    private final List<ScoreField> scoreSelectors = new ArrayList<>();

    public String getMatchType() {
        return matchType;
    }

    public boolean hasMatchType() {
        return matchType != null;
    }

    public NeuronsMatchFilter<R> setMatchType(String matchType) {
        this.matchType = matchType;
        return this;
    }

    public List<ScoreField> getScoreSelectors() {
        return scoreSelectors;
    }

    public NeuronsMatchFilter<R> addSScore(String scoreField, double minScore) {
        scoreSelectors.add(new ScoreField(scoreField, minScore));
        return this;
    }

    public boolean isEmpty() {
        return matchType == null && scoreSelectors.isEmpty();
    }

}
