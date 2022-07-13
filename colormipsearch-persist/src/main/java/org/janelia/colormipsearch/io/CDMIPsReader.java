package org.janelia.colormipsearch.io;

import java.util.List;

import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public interface CDMIPsReader {
    List<? extends AbstractNeuronMetadata> readMIPs(String library, long offset, int length);
}
