package org.janelia.colormipsearch.model;

public abstract class AbstractMatch<I extends AbstractNeuronImage, M extends AbstractNeuronImage> {
    private I input;
    private AbstractMatchResult<M> match;
}
