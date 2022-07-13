package org.janelia.colormipsearch.io;

import java.util.List;

import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;

public interface CDMatchesReader<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> {
    /**
     * This method will list the location for all color depth matches.
     *
     * A file based implementation will return all files that contain CD matches.
     *
     * @return
     */
    List<String> listCDMatchesLocations();
    List<CDMatch<M, T>> readCDMatches(String maskSource);
}
