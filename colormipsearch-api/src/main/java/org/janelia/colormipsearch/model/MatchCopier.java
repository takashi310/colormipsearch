package org.janelia.colormipsearch.model;

public interface MatchCopier<M1 extends AbstractNeuronMetadata,
        T1 extends AbstractNeuronMetadata,
        R1 extends AbstractMatch<M1, T1>,
        M2 extends AbstractNeuronMetadata,
        T2 extends AbstractNeuronMetadata,
        R2 extends AbstractMatch<M2, T2>> {
    void copy(R1 src, R2 dest);
}
