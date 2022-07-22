package org.janelia.colormipsearch.model;

public interface MatchCopier<R1 extends AbstractMatch<? extends AbstractNeuronMetadata, ? extends AbstractNeuronMetadata>,
                             R2 extends AbstractMatch<? extends AbstractNeuronMetadata, ? extends AbstractNeuronMetadata>> {
    void copy(R1 src, R2 dest);
}
