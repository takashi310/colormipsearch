package org.janelia.colormipsearch.dataio.fs;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectWriter;

import org.janelia.colormipsearch.dataio.ResultMatchesWriter;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.results.MatchResultsGrouping;

public class JSONCDSResultsWriter<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata>
        extends AbstractJSONCDSWriter<M, T>
        implements ResultMatchesWriter<M, T, CDMatch<M, T>> {

    private final Path perMasksOutputDir;
    private final Path perMatchesOutputDir;

    public JSONCDSResultsWriter(ObjectWriter jsonWriter,
                                Path perMasksOutputDir,
                                Path perMatchesOutputDir) {
        super(jsonWriter);
        this.perMatchesOutputDir = perMatchesOutputDir;
        this.perMasksOutputDir = perMasksOutputDir;
    }

    public void write(List<CDMatch<M, T>> CDMatches) {
        // write results by mask ID (creating the collection right before it's passed as and arg in order to type match)
        writeAllSearchResults(
                MatchResultsGrouping.groupByMaskFields(
                        CDMatches,
                        Collections.singletonList(
                                AbstractNeuronMetadata::getId
                        ),
                        Comparator.comparingDouble(m -> -m.getMatchingPixels())
                ),
                perMasksOutputDir
        );

        // write results by matched ID (creating the collection right before it's passed as and arg in order to type match)
        writeAllSearchResults(
                MatchResultsGrouping.groupByMatchedFields(
                        CDMatches,
                        Collections.singletonList(
                                AbstractNeuronMetadata::getId
                        ),
                        Comparator.comparingDouble(m -> -m.getMatchingPixels())
                ),
                perMatchesOutputDir
        );
    }
}