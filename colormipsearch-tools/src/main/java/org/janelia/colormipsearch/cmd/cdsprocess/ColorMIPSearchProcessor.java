package org.janelia.colormipsearch.cmd.cdsprocess;

import java.util.List;

import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDSMatch;

public interface ColorMIPSearchProcessor<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> {
    List<CDSMatch<M, T>> findAllColorDepthMatches(List<M> queryMIPs, List<T> targetMIPs);

    void terminate();
}
