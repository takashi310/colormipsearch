package org.janelia.colormipsearch.dataio.fs;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.dataio.NeuronMatchesUpdater;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.results.MatchResultsGrouping;

public class JSONCDSMatchesUpdater<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata>
        extends AbstractJSONCDSMatchesWriter<M, T>
        implements NeuronMatchesUpdater<M, T, CDMatch<M, T>> {
    private final Path outputDir;

    public JSONCDSMatchesUpdater(ObjectWriter jsonWriter,
                                 Path outputDir) {
        super(jsonWriter);
        this.outputDir = outputDir;
    }

    @Override
    public void writeUpdates(List<CDMatch<M, T>> matches, List<Function<CDMatch<M, T>, Pair<String, ?>>> fieldSelectors) {
        // write results by mask ID, ignoring the field selectors since we are writing all the results
        writeAllSearchResults(
                MatchResultsGrouping.groupByMaskFields(
                        matches,
                        Collections.singletonList(
                                AbstractNeuronMetadata::getId
                        ),
                        Comparator.comparingDouble(m -> -m.getNormalizedScore())
                ),
                outputDir
        );
    }
}