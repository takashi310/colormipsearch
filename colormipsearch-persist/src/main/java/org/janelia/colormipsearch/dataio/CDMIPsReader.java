package org.janelia.colormipsearch.dataio;

import java.util.List;

import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public interface CDMIPsReader {
    List<? extends AbstractNeuronMetadata> readMIPs(DataSourceParam inputMipsParam);
}
