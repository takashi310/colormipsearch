package org.janelia.colormipsearch.dataio;

import java.util.List;

import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

/**
 * Color Depth MIP metadata reader.
 */
public interface CDMIPsReader {
    /**
     *
     * @param inputMipsParam mips source and the interpretation of the datasource depends on the implementation.
     *                       The datasource may be a directory containing JSON files or a single file.
     * @return a list of MIPs metadata.
     */
    List<? extends AbstractNeuronMetadata> readMIPs(DataSourceParam inputMipsParam);
}
