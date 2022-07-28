package org.janelia.colormipsearch.cmd.v2dataimport;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.api_v2.cdsearch.CDSMatches;
import org.janelia.colormipsearch.api_v2.cdsearch.ColorMIPSearchResultUtils;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.datarequests.ScoresFilter;
import org.janelia.colormipsearch.datarequests.SortCriteria;
import org.janelia.colormipsearch.model.CDMatchEntity;
import org.janelia.colormipsearch.model.ComputeFileType;
import org.janelia.colormipsearch.model.EMNeuronEntity;
import org.janelia.colormipsearch.model.FileData;
import org.janelia.colormipsearch.model.FileType;
import org.janelia.colormipsearch.model.LMNeuronEntity;

public class JSONV2Em2LmMatchesReader implements NeuronMatchesReader<CDMatchEntity<EMNeuronEntity, LMNeuronEntity>> {

    private final ObjectMapper mapper;

    public JSONV2Em2LmMatchesReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public List<String> listMatchesLocations(List<DataSourceParam> matchesSource) {
        return matchesSource.stream()
                .flatMap(arg -> listFiles(arg.getLocation(), (int) arg.getOffset(), arg.getSize()).stream())
                .collect(Collectors.toList());
    }

    private List<String> listFiles(String location, int offsetParam, int lengthParam) {
        try {
            Path pathLocation = Paths.get(location);
            if (Files.isRegularFile(pathLocation)) {
                return Collections.singletonList(pathLocation.toString());
            } else if (Files.isDirectory(pathLocation)) {
                int from = Math.max(offsetParam, 0);
                List<String> filenamesList = Files.find(pathLocation, 1, (p, fa) -> fa.isRegularFile())
                        .skip(from)
                        .map(Path::toString)
                        .collect(Collectors.toList());
                if (lengthParam > 0 && lengthParam < filenamesList.size()) {
                    return filenamesList.subList(0, lengthParam);
                } else {
                    return filenamesList;
                }
            } else {
                return Collections.emptyList();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public List<CDMatchEntity<EMNeuronEntity, LMNeuronEntity>> readMatchesForMasks(String maskLibrary, List<String> maskMipIds, ScoresFilter matchScoresFilter, List<SortCriteria> sortCriteriaList) {
        return maskMipIds.stream()
                .map(maskMipId -> StringUtils.isNotBlank(maskLibrary) ? Paths.get(maskLibrary, maskMipId).toFile() : new File(maskMipId))
                .map(this::readEMMatches)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public List<CDMatchEntity<EMNeuronEntity, LMNeuronEntity>> readMatchesForTargets(String targetLibrary, List<String> targetMipIds, ScoresFilter matchScoresFilter, List<SortCriteria> sortCriteriaList) {
        throw new UnsupportedOperationException("This class has very limitted support and it is only intended for import EM to LM matches based on the EM MIP ID(s)");
    }

    private List<CDMatchEntity<EMNeuronEntity, LMNeuronEntity>> readEMMatches(File f) {
        CDSMatches matchesFileContent = ColorMIPSearchResultUtils.readCDSMatchesFromJSONFile(f, mapper);
        if (matchesFileContent == null || matchesFileContent.isEmpty()) {
            return Collections.emptyList();
        } else {
            return matchesFileContent.getResults().stream()
                    .map(v2CDMatch -> {
                        CDMatchEntity<EMNeuronEntity, LMNeuronEntity> cdMatch = new CDMatchEntity<>();
                        EMNeuronEntity emNeuronMetadata = new EMNeuronEntity();
                        emNeuronMetadata.setMipId(v2CDMatch.getSourceId());
                        emNeuronMetadata.setLibraryName(v2CDMatch.getSourceLibraryName());

                        emNeuronMetadata.setComputeFileData(ComputeFileType.SourceColorDepthImage,
                                FileData.fromString(v2CDMatch.getSourceCdmPath()));
                        emNeuronMetadata.setComputeFileData(ComputeFileType.InputColorDepthImage,
                                FileData.fromString(v2CDMatch.getSourceImageName()));

                        emNeuronMetadata.setNeuronFileData(FileType.ColorDepthMipInput,
                                FileData.fromString(v2CDMatch.getSourceSearchablePNG()));

                        cdMatch.setMaskImage(emNeuronMetadata);

                        LMNeuronEntity lmNeuronMetadata = new LMNeuronEntity();
                        lmNeuronMetadata.setMipId(v2CDMatch.getId());
                        lmNeuronMetadata.setLibraryName(v2CDMatch.getLibraryName());

                        lmNeuronMetadata.setComputeFileData(ComputeFileType.SourceColorDepthImage,
                                FileData.fromString(v2CDMatch.getCdmPath()));
                        lmNeuronMetadata.setComputeFileData(ComputeFileType.InputColorDepthImage,
                                FileData.fromString(v2CDMatch.getImageName()));

                        lmNeuronMetadata.setNeuronFileData(FileType.ColorDepthMipInput,
                                FileData.fromString(v2CDMatch.getSearchablePNG()));

                        cdMatch.setMatchedImage(lmNeuronMetadata);

                        // setup the scores
                        cdMatch.setNormalizedScore((float) v2CDMatch.getNormalizedScore());
                        cdMatch.setMatchingPixels(v2CDMatch.getMatchingPixels());
                        cdMatch.setMatchingPixelsRatio((float) v2CDMatch.getMatchingRatio());
                        cdMatch.setGradientAreaGap(v2CDMatch.getGradientAreaGap());
                        cdMatch.setHighExpressionArea(v2CDMatch.getHighExpressionArea());
                        return cdMatch;
                    })
                    .collect(Collectors.toList());
        }
    }

}
