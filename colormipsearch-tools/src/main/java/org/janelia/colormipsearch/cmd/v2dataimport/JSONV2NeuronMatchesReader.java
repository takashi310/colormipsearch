package org.janelia.colormipsearch.cmd.v2dataimport;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.datarequests.ScoresFilter;
import org.janelia.colormipsearch.datarequests.SortCriteria;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;

public class JSONV2NeuronMatchesReader<R extends AbstractMatch<? extends AbstractNeuronMetadata,
                                                               ? extends AbstractNeuronMetadata>> implements NeuronMatchesReader<R> {

    private final ObjectMapper mapper;

    public JSONV2NeuronMatchesReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public List<String> listMatchesLocations(List<DataSourceParam> matchesSource) {
        return null; // FIXME !!!
    }

    @Override
    public List<R> readMatchesForMasks(String maskLibrary, List<String> maskMipIds, ScoresFilter matchScoresFilter, List<SortCriteria> sortCriteriaList) {
        return null; // FIXME !!!
    }

    @Override
    public List<R> readMatchesForTargets(String targetLibrary, List<String> targetMipIds, ScoresFilter matchScoresFilter, List<SortCriteria> sortCriteriaList) {
        return null; // FIXME !!!
    }
}
