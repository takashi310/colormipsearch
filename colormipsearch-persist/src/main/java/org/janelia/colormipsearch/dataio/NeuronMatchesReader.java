package org.janelia.colormipsearch.dataio;

import java.util.List;

import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;

public interface NeuronMatchesReader<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>> {
    /**
     * This method will list the location for all color depth matches.
     *
     * A file based implementation will return all files that contain CD matches.
     *
     * @param matchInputs
     * @return
     */
    List<String> listMatchesLocations(List<InputParam> matchInputs);
    List<R> readMatches(String maskSource);
}
