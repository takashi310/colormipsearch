package org.janelia.colormipsearch.cmd.cdsprocess;

import java.util.List;

import org.janelia.colormipsearch.mips.NeuronMIP;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDSMatch;

public interface ColorMIPSearchProcessor<M extends AbstractNeuronMetadata, I extends AbstractNeuronMetadata> {
    List<CDSMatch<M, I>> findAllColorDepthMatches(List<M> queryMIPs, List<I> targetMIPs);
    void terminate();
}
