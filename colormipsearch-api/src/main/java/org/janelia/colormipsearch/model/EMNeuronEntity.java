package org.janelia.colormipsearch.model;

import org.janelia.colormipsearch.dto.EMNeuronMetadata;

public class EMNeuronEntity extends AbstractNeuronEntity {

    @Override
    public String getNeuronId() {
        return getPublishedName();
    }

    @Override
    public EMNeuronEntity duplicate() {
        EMNeuronEntity n = new EMNeuronEntity();
        n.copyFrom(this);
        return n;
    }

    @Override
    public EMNeuronMetadata metadata() {
        EMNeuronMetadata n = new EMNeuronMetadata();
        n.setInternalId(getEntityId());
        n.setAlignmentSpace(getAlignmentSpace());
        n.setMipId(getMipId());
        n.setLibraryName(getLibraryName());
        n.setPublishedName(getPublishedName());
        getComputeFiles().forEach((ft, fd) -> n.setNeuronComputeFile(ft, fd.getFileName()));
        getProcessedTags().forEach(n::putProcessedTags);
        return n;
    }
}
