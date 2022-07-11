package org.janelia.colormipsearch.cmd.cdsprocess;

import java.util.List;

import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;

public interface ColorMIPSearchProcessor<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> {
    List<CDMatch<M, T>> findAllColorDepthMatches(List<M> queryMIPs, List<T> targetMIPs);

    void terminate();
}
