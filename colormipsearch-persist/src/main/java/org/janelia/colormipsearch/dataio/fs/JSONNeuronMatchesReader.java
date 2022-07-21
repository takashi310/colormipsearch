package org.janelia.colormipsearch.dataio.fs;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.janelia.colormipsearch.dao.NeuronsMatchFilter;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.FileType;
import org.janelia.colormipsearch.model.MatchComputeFileType;
import org.janelia.colormipsearch.model.PPPMatch;
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

    @Override
    public List<R> readMatches(String filename, NeuronsMatchFilter<R> matchesFilter) {
        ResultMatches<M, T, R> matches = readMatchesResults(new File(filename));
        return convertMatchesResultsToListOfMatches(matches);
    }

    private ResultMatches<M, T, R> readMatchesResults(File f) {
        try {
            return mapper.readValue(f, new TypeReference<ResultMatches<M, T, R>>() {});
        } catch (IOException e) {
            throw new UncheckedIOException("Error reading CDSMatches from JSON file:" + f, e);
        }
    }

    @SuppressWarnings("unchecked")
    private List<R> convertMatchesResultsToListOfMatches(ResultMatches<M, T, R> matchesResults) {
        return matchesResults.getItems().stream()
                .map(persistedMatch -> {
                    return (R) persistedMatch.duplicate((src, dest) -> {
                        M maskImage = matchesResults.getKey().duplicate();
                        T targetImage = persistedMatch.getMatchedImage();
                        maskImage.setComputeFileData(ComputeFileType.InputColorDepthImage,
                                persistedMatch.getMatchComputeFileData(MatchComputeFileType.MaskColorDepthImage));
                        maskImage.setComputeFileData(ComputeFileType.GradientImage,
                                persistedMatch.getMatchComputeFileData(MatchComputeFileType.MaskGradientImage));
                        maskImage.setComputeFileData(ComputeFileType.ZGapImage,
                                persistedMatch.getMatchComputeFileData(MatchComputeFileType.MaskZGapImage));
                        maskImage.setNeuronFileData(FileType.ColorDepthMipInput,
                                persistedMatch.getMatchFileData(FileType.ColorDepthMipInput));
                        dest.setMaskImage(maskImage);
                        dest.setMatchedImage(targetImage);
                        // no reason to keep these around
                        dest.resetMatchComputeFiles();
                        dest.resetMatchFiles();
                    });
                })
                .collect(Collectors.toList());
    }
}
