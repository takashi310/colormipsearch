package org.janelia.colormipsearch.cmd.cdsprocess;

import java.util.List;

import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.CDMatchEntity;

public interface ColorMIPSearchProcessor<M extends AbstractNeuronEntity, T extends AbstractNeuronEntity> {
    List<CDMatchEntity<M, T>> findAllColorDepthMatches(List<M> queryMIPs, List<T> targetMIPs);

    void terminate();
}
