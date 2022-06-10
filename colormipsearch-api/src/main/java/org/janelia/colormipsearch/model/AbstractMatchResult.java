package org.janelia.colormipsearch.model;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractMatchResult<I extends AbstractNeuronImage> {
    private I image;
    private boolean mirrored;
    private Map<FileType, FileData> matchFiles = new HashMap<>();
}
