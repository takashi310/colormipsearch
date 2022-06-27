package org.janelia.colormipsearch.cmd.io;

import java.util.List;

import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDSMatch;

public interface ResultMatchesWriter<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>> {
    void write(List<R> matches);
}
