package org.janelia.colormipsearch.cmd.cdsprocess;

import java.util.List;
import java.util.stream.Collectors;

import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.janelia.colormipsearch.results.ScoredEntry;

public class ColorMIPProcessUtils {
    public static <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity> List<CDMatch<M, T>> selectBestMatches(List<CDMatch<M, T>> CDMatches,
                                                                                                                         int topLineMatches,
                                                                                                                         int topSamplesPerLine,
                                                                                                                         int topMatchesPerSample) {
        List<ScoredEntry<List<CDMatch<M, T>>>> topRankedLineMatches = ItemsHandling.selectTopRankedElements(
                CDMatches,
                match -> match.getMatchedImage().getPublishedName(),
                CDMatch::getMatchingPixels,
                topLineMatches,
                -1);

        return topRankedLineMatches.stream()
                .flatMap(se -> ItemsHandling.selectTopRankedElements( // topRankedSamplesPerLine
                        se.getEntry(),
                        match -> match.getMatchedImage().getNeuronId(),
                        CDMatch::getMatchingPixels,
                        topSamplesPerLine,
                        topMatchesPerSample
                ).stream())
                .flatMap(se -> se.getEntry().stream())
                .collect(Collectors.toList())
                ;
    }
}
