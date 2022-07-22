package org.janelia.colormipsearch.dataio.fs;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.dataio.NeuronMatchesUpdater;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.results.MatchResultsGrouping;
import org.janelia.colormipsearch.results.ResultMatches;

public class JSONCDSMatchesUpdater<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata>
        implements NeuronMatchesUpdater<M, T, CDMatch<M, T>> {

    private final JSONResultMatchesWriter resultMatchesWriter;
    private final Path outputDir;

    public JSONCDSMatchesUpdater(ObjectWriter jsonWriter,
                                 Path outputDir) {
        this.resultMatchesWriter = new JSONResultMatchesWriter(jsonWriter);
        this.outputDir = outputDir;
    }

    @Override
    public void writeUpdates(List<CDMatch<M, T>> matches, List<Function<CDMatch<M, T>, Pair<String, ?>>> fieldSelectors) {
        // write results by mask ID, ignoring the field selectors since we are writing all the results
        List<Function<M, ?>> grouping = Collections.singletonList(
                AbstractNeuronMetadata::getId
        );
        Comparator<CDMatch<M, T>> ordering = Comparator.comparingDouble(m -> -m.getNormalizedScore());
        List<ResultMatches<M, T, CDMatch<M, T>>> resultMatches = MatchResultsGrouping.groupByMaskFields(
                matches,
                grouping,
                ordering
        );
        resultMatchesWriter.writeResultMatchesList(resultMatches, AbstractNeuronMetadata::getId, outputDir);
    }
}