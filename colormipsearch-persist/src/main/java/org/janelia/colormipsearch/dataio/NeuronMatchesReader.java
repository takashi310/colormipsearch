package org.janelia.colormipsearch.dataio;

import java.util.List;

import org.janelia.colormipsearch.datarequests.ScoresFilter;
import org.janelia.colormipsearch.datarequests.SortCriteria;
import org.janelia.colormipsearch.model.AbstractMatchEntity;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;

public interface NeuronMatchesReader<R extends AbstractMatchEntity<? extends AbstractNeuronEntity, ? extends AbstractNeuronEntity>> {
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
     * @param alignmentSpace masks alignmentSpace
     * @param maskLibrary masks library
     * @param maskMipIds mask MIPs
     * @param matchScoresFilter additional matches scores filter
     * @return
     */
    List<R> readMatchesForMasks(String alignmentSpace,
                                String maskLibrary,
                                List<String> maskMipIds,
                                ScoresFilter matchScoresFilter,
                                List<SortCriteria> sortCriteriaList);

    /**
     * Read matches for the specified targets library and mips. A null or empty list will ignore that filter.
     *
     * @param alignmentSpace targets alignmentSpace
     * @param targetLibrary masks library
     * @param targetMipIds mask MIPs
     * @param matchScoresFilter additional match scores filter
     * @return
     */
    List<R> readMatchesForTargets(String alignmentSpace,
                                  String targetLibrary,
                                  List<String> targetMipIds,
                                  ScoresFilter matchScoresFilter,
                                  List<SortCriteria> sortCriteriaList);
}
