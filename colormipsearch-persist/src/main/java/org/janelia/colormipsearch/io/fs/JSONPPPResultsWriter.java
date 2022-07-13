package org.janelia.colormipsearch.io.fs;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectWriter;

import org.janelia.colormipsearch.io.JsonOutputHelper;
import org.janelia.colormipsearch.io.ResultMatchesWriter;
import org.janelia.colormipsearch.io.fs.FSUtils;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.PPPMatch;
import org.janelia.colormipsearch.results.MatchResultsGrouping;
import org.janelia.colormipsearch.results.ResultMatches;

public class JSONPPPResultsWriter<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> implements ResultMatchesWriter<M, T, PPPMatch<M, T>> {

    private final ObjectWriter jsonWriter;
    private final Path outputDir;

    public JSONPPPResultsWriter(ObjectWriter jsonWriter, Path outputDir) {
        this.jsonWriter = jsonWriter;
        this.outputDir = outputDir;
    }

    public void write(List<PPPMatch<M, T>> pppMatches) {
        List<Function<M, ?>> fieldSelectors = Collections.singletonList(
                AbstractNeuronMetadata::getPublishedName
        );

        List<ResultMatches<M, T, PPPMatch<M, T>>> resultsByNeuronId = MatchResultsGrouping.groupByMaskFields(
                pppMatches,
                fieldSelectors,
                Comparator.comparingDouble(aPPPMatch -> Math.abs(aPPPMatch.getRank())));
        if (resultsByNeuronId.size() > 1) {
            throw new IllegalStateException("Expected all PPP matches to be for the same neuron");
        } else if (resultsByNeuronId.size() == 1) {
            FSUtils.createDirs(outputDir);
            ResultMatches<M, T, PPPMatch<M, T>> neuronPPPMatches = resultsByNeuronId.get(0);
            JsonOutputHelper.writeToJSONFile(
                    neuronPPPMatches,
                    FSUtils.getOutputFile(outputDir, new File(neuronPPPMatches.getKey().getPublishedName() + ".json")),
                    jsonWriter);
        }
    }

}
