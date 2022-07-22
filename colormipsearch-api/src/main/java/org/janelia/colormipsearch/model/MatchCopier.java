package org.janelia.colormipsearch.model;

/**
 * Object responsible for copying fields from one type of match to a different type of match.
 * This is typically used to switch masks with targets,
 * for example to copy from CDMatch&lt;M, T&gt; to CDMatch&lt;T, M&gt;
 *
 * @param <R1>
 * @param <R2>
 */
public interface MatchCopier<R1 extends AbstractMatch<? extends AbstractNeuronMetadata, ? extends AbstractNeuronMetadata>,
                             R2 extends AbstractMatch<? extends AbstractNeuronMetadata, ? extends AbstractNeuronMetadata>> {
    void copy(R1 src, R2 dest);
}
