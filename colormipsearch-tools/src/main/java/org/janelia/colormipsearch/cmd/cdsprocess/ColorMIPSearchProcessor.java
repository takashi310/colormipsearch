package org.janelia.colormipsearch.cmd.cdsprocess;

import java.util.List;

import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.CDMatch;

public interface ColorMIPSearchProcessor<M extends AbstractNeuronEntity, T extends AbstractNeuronEntity> {
    List<CDMatch<M, T>> findAllColorDepthMatches(List<M> queryMIPs, List<T> targetMIPs);

    void terminate();
}
