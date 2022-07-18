package org.janelia.colormipsearch.dataio.fs;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectWriter;

import org.janelia.colormipsearch.dataio.ResultMatchesUpdatesWriter;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.results.MatchResultsGrouping;

public class JSONCDSUpdatesWriter<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata>
        extends AbstractJSONCDSWriter<M, T>
        implements ResultMatchesUpdatesWriter<M, T, CDMatch<M, T>> {
    private final Path outputDir;

    public JSONCDSUpdatesWriter(ObjectWriter jsonWriter,
                                Path outputDir) {
        super(jsonWriter);
        this.outputDir = outputDir;
    }

    @Override
    public void writeUpdates(List<CDMatch<M, T>> matches) {
        // write results by mask ID (creating the collection right before it's passed as and arg in order to type match)
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