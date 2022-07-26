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
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.results.MatchResultsGrouping;
import org.janelia.colormipsearch.results.ResultMatches;

public class JSONNeuronMatchesByMaskUpdater<M extends AbstractNeuronEntity,
                                            T extends AbstractNeuronEntity,
                                            R extends AbstractMatch<M, T>>
        implements NeuronMatchesUpdater<R> {

    private final JSONResultMatchesWriter resultMatchesWriter;
    private final Comparator<AbstractMatch<?, ?>> matchOrdering;
    private final Path outputDir;

    public JSONNeuronMatchesByMaskUpdater(ObjectWriter jsonWriter,
                                          Comparator<AbstractMatch<?, ?>> matchOrdering,
                                          Path outputDir) {
        this.resultMatchesWriter = new JSONResultMatchesWriter(jsonWriter);
        this.matchOrdering = matchOrdering;
        this.outputDir = outputDir;
    }

    @Override
    public void writeUpdates(List<R> matches, List<Function<R, Pair<String, ?>>> fieldSelectors) {
        // write results by mask ID, ignoring the field selectors since we are writing all the results
        List<Function<M, ?>> grouping = Collections.singletonList(
                AbstractNeuronEntity::getMipId
        );
        Comparator<R> ordering = matchOrdering::compare;
        List<ResultMatches<M, T, R>> resultMatches = MatchResultsGrouping.groupByMaskFields(
                matches,
                grouping,
                ordering
        );
        resultMatchesWriter.writeResultMatchesList(resultMatches, AbstractNeuronEntity::getMipId, outputDir);
    }
}