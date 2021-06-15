package org.janelia.colormipsearch.api.pppsearch;

public class SkeletonMatch {
    private String id;
    private Double nblastScore;
    private Double coverage;
    private short[] colors;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Double getNblastScore() {
        return nblastScore;
    }

    public void setNblastScore(Double nblastScore) {
        this.nblastScore = nblastScore;
    }

    public Double getCoverage() {
        return coverage;
    }

    public void setCoverage(Double coverage) {
        this.coverage = coverage;
    }

    public short[] getColors() {
        return colors;
    }

    public void setColors(short[] colors) {
        this.colors = colors;
    }
}
