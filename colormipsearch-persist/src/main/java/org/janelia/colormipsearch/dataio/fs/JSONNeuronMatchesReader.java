package org.janelia.colormipsearch.dataio.fs;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.dao.NeuronsMatchFilter;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.datarequests.ScoresFilter;
import org.janelia.colormipsearch.datarequests.SortCriteria;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.FileType;
import org.janelia.colormipsearch.model.MatchComputeFileType;
import org.janelia.colormipsearch.model.PPPMatch;
import org.janelia.colormipsearch.results.MatchResultsGrouping;
import org.janelia.colormipsearch.results.ResultMatches;

public class JSONNeuronMatchesReader<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>> implements NeuronMatchesReader<M, T, R> {
    private final ObjectMapper mapper;

    public JSONNeuronMatchesReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public List<String> listMatchesLocations(List<DataSourceParam> matchesSource) {
        return matchesSource.stream()
                .flatMap(arg -> FSUtils.getFiles(arg.getLocation(), (int) arg.getOffset(), arg.getSize()).stream())
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<R> readMatchesForMasks(String maskLibrary, List<String> maskMipIds,
                                       ScoresFilter matchScoresFilter,
                                       List<SortCriteria> sortCriteriaList) {
        return (List<R>) maskMipIds.stream()
                .map(maskMipId -> StringUtils.isNotBlank(maskLibrary) ? Paths.get(maskLibrary, maskMipId).toFile() : new File(maskMipId))
                .map(this::readMatchesResults)
                .flatMap(resultMatches -> MatchResultsGrouping.expandResultsByMask(resultMatches).stream())
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<R> readMatchesForTargets(String targetLibrary, List<String> targetMipIds,
                                         ScoresFilter matchScoresFilter,
                                         List<SortCriteria> sortCriteriaList) {
        return (List<R>) targetMipIds.stream()
                .map(targetMipId -> StringUtils.isNotBlank(targetLibrary) ? Paths.get(targetLibrary, targetMipId).toFile() : new File(targetMipId))
                .map(this::readMatchesResults)
                .flatMap(resultMatches -> MatchResultsGrouping.expandResultsByTarget(resultMatches).stream())
                .collect(Collectors.toList());
    }

    private <M1 extends AbstractNeuronMetadata, T1 extends AbstractNeuronMetadata, R1 extends AbstractMatch<M1, T1>> ResultMatches<M1, T1, R1> readMatchesResults(File f) {
        try {
            return mapper.readValue(f, new TypeReference<ResultMatches<M1, T1, R1>>() {});
        } catch (IOException e) {
            throw new UncheckedIOException("Error reading CDSMatches from JSON file:" + f, e);
        }
    }

}
