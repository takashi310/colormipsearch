package org.janelia.colormipsearch.dataio.fs;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectWriter;

import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.results.ResultMatches;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JSONResultMatchesWriter {
    private static final Logger LOG = LoggerFactory.getLogger(JSONResultMatchesWriter.class);

    private final ObjectWriter jsonWriter;

    JSONResultMatchesWriter(ObjectWriter jsonWriter) {
        this.jsonWriter = jsonWriter;
    }

    <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity, R extends AbstractMatch<M, T>>
    void writeResultMatchesList(List<ResultMatches<M, T, R>> resultMatchesList,
                                Function<AbstractNeuronEntity, String> filenameSelector,
                                Path outputDir) {
        long startTime = System.currentTimeMillis();
        FSUtils.createDirs(outputDir);
        LOG.info("Write {} file results to {}", resultMatchesList.size(), outputDir);
        resultMatchesList.stream().parallel()
                .forEach(resultMatches -> writeResultMatches(resultMatches, filenameSelector, outputDir));
        LOG.info("Finished writing {} file results in {}s", resultMatchesList.size(), (System.currentTimeMillis() - startTime) / 1000.);
    }

    <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity, R extends AbstractMatch<M, T>>
    void writeResultMatches(ResultMatches<M, T, R> resultMatches,
                            Function<AbstractNeuronEntity, String> filenameSelector,
                            Path outputDir) {
        String filename = filenameSelector.apply(resultMatches.getKey());
        JsonOutputHelper.writeToJSONFile(
                resultMatches,
                FSUtils.getOutputPath(outputDir, new File(filename) + ".json"),
                jsonWriter);
    }
}