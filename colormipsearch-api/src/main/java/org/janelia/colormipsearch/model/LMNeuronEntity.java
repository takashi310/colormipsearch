package org.janelia.colormipsearch.model;

import org.janelia.colormipsearch.dto.LMNeuronMetadata;

public class LMNeuronEntity extends AbstractNeuronEntity {

    // LM slide code is required for selecting the top ranked matches during gradient scoring
    private String slideCode;

    @Override
    public String getNeuronId() {
        return getSlideCode();
    }

    public String getSlideCode() {
        return slideCode;
    }

    public void setSlideCode(String slideCode) {
        this.slideCode = slideCode;
    }

    @Override
    public LMNeuronEntity duplicate() {
        LMNeuronEntity n = new LMNeuronEntity();
        n.copyFrom(this);
        n.slideCode = this.getSlideCode();
        return n;
    }

    @Override
    public LMNeuronMetadata metadata() {
        LMNeuronMetadata n = new LMNeuronMetadata();
        n.setAlignmentSpace(getAlignmentSpace());
        n.setMipId(getMipId());
        n.setLibraryName(getLibraryName());
        n.setPublishedName(getPublishedName());
        n.setSlideCode(slideCode);
        return n;
    }
}
