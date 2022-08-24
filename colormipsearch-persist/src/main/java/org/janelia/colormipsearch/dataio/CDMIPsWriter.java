package org.janelia.colormipsearch.dataio;

import java.util.List;

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
     * Write all items.
     *
     * @param neuronMetadata items to write
     */
    void write(List<AbstractNeuronEntity> neuronMetadata);

    /**
     * Write one item.
     *
     * @param neuronMetadata item to write
     */
    void writeOne(AbstractNeuronEntity neuronMetadata);

    /**
     * Finish all writes and close the writer.
     */
    void close();
}
