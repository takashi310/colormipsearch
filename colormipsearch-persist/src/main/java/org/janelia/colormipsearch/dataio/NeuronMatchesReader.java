package org.janelia.colormipsearch.dataio;

import java.util.List;

import org.janelia.colormipsearch.dao.NeuronsMatchFilter;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public interface NeuronMatchesReader<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>> {
    /**
     * This method will list the location for all color depth matches.
     *
     * A file based implementation will return all files that contain CD matches.
     * A database based implementation will return  all distinct MIP IDs from a library.
     *
     * @param matchesSource
     * @return
     */
    List<String> listMatchesLocations(List<DataSourceParam> matchesSource);

    /**
     * Read matches for the specified masks library and mips. A null or empty list will ignore that filter.
     *
     * @param maskLibrary masks library
     * @param maskMipIds mask MIPs
     * @param matchesFilter additional matches filter
     * @return
     */
    List<R> readMatchesForMasks(String maskLibrary, List<String> maskMipIds,
                                NeuronsMatchFilter<R> matchesFilter);

    /**
     * Read matches for the specified targets library and mips. A null or empty list will ignore that filter.
     *
     * @param targetLibrary masks library
     * @param targetMipIds mask MIPs
     * @param matchesFilter additional matches filter
     * @return
     */
    List<R> readMatchesForTargets(String targetLibrary, List<String> targetMipIds,
                                  NeuronsMatchFilter<R> matchesFilter);
}
