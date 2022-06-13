package org.janelia.colormipsearch.api.cds;

import org.janelia.colormipsearch.api.imageprocessing.ImageArray;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public class NeuronMIP<N extends AbstractNeuronMetadata> {
    private final N neuronInfo;
    private final ImageArray<?> imageArray;

    public NeuronMIP(N neuronInfo, ImageArray<?> imageArray) {
        this.neuronInfo = neuronInfo;
        this.imageArray = imageArray;
    }

    public N getNeuronInfo() {
        return neuronInfo;
    }

    public ImageArray<?> getImageArray() {
        return imageArray;
    }
}
