package org.janelia.colormipsearch.dataio;

import java.util.List;

import org.janelia.colormipsearch.model.AbstractNeuronEntity;

/**
 * Color Depth MIP metadata reader.
 */
public interface CDMIPsReader {
    /**
     * @param mipsDataSource mips source and the interpretation of the datasource depends on the implementation.
     * @return a list of MIPs metadata.
     */
    List<? extends AbstractNeuronEntity> readMIPs(DataSourceParam mipsDataSource);
}
