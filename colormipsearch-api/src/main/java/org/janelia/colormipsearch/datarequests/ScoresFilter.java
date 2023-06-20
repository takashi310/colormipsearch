package org.janelia.colormipsearch.datarequests;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

public class ScoresFilter {
    public static class ScoreField {
        private final String fieldName; // score field name
        private final Double minScore; // min score value

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

    private final List<ScoreField> scoreSelectors = new ArrayList<>();

    public List<ScoreField> getScoreSelectors() {
        return scoreSelectors;
    }

    public ScoresFilter addSScore(String scoreField, double minScore) {
        scoreSelectors.add(new ScoreField(scoreField, minScore));
        return this;
    }

    public boolean isEmpty() {
        return scoreSelectors.isEmpty();
    }

}
