package org.janelia.colormipsearch.dataio;

import org.janelia.colormipsearch.model.AbstractNeuronEntity;

/**
 * Writer for Color Depth MIPs metadata.
 */
public interface CDMIPsWriter {
    /**
     * Open and prepare the writer.
     */
    void open();

    /**
     * Write a single item.
     *
     * @param neuronMetadata item to write
     */
    void write(AbstractNeuronEntity neuronMetadata);

    /**
     * Finish all writes and close the writer.
     */
    void close();
}
