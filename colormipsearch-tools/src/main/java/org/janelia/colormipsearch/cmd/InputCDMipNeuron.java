package org.janelia.colormipsearch.cmd;

import javax.annotation.Nonnull;

import org.janelia.colormipsearch.cmd.jacsdata.ColorDepthMIP;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;

/**
 * This class is used for generating and persisting all corresponding neuron metadata from a color depth MIP.
 * From a single color depth MIP there may be multiple neuron entities because of LM segmented images or EM flipped neurons.
 *
 * @param <N> neuron entity type
 */
class InputCDMipNeuron<N extends AbstractNeuronEntity> {

    // source color depth MIP metadata
    private final ColorDepthMIP sourceMIP;
    // neuron metadata generated from source color depth MIP
    private final N neuronMetadata;

    InputCDMipNeuron(@Nonnull ColorDepthMIP sourceMIP, @Nonnull N neuronMetadata) {
        this.sourceMIP = sourceMIP;
        this.neuronMetadata = neuronMetadata;
    }

    ColorDepthMIP getSourceMIP() {
        return sourceMIP;
    }

    N getNeuronMetadata() {
        return neuronMetadata;
    }
}
