package org.janelia.colormipsearch.dataio.fs;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectWriter;

import org.janelia.colormipsearch.model.AbstractMatchEntity;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.results.GroupedMatchedEntities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JSONResultMatchesWriter {
    private static final Logger LOG = LoggerFactory.getLogger(JSONResultMatchesWriter.class);

    private final ObjectWriter jsonWriter;

    JSONResultMatchesWriter(ObjectWriter jsonWriter) {
        this.jsonWriter = jsonWriter;
    }

    <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity, R extends AbstractMatchEntity<M, T>>
    void writeResultMatchesList(List<GroupedMatchedEntities<M, T, R>> groupedMatchedEntitiesList,
                                Function<AbstractNeuronEntity, String> filenameSelector,
                                Path outputDir) {
        long startTime = System.currentTimeMillis();
        FSUtils.createDirs(outputDir);
        LOG.info("Write {} file results to {}", groupedMatchedEntitiesList.size(), outputDir);
        groupedMatchedEntitiesList.stream().parallel()
                .forEach(resultMatches -> writeResultMatches(resultMatches, filenameSelector, outputDir));
        LOG.info("Finished writing {} file results in {}s", groupedMatchedEntitiesList.size(), (System.currentTimeMillis() - startTime) / 1000.);
    }

    <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity, R extends AbstractMatchEntity<M, T>>
    void writeResultMatches(GroupedMatchedEntities<M, T, R> groupedMatchedEntities,
                            Function<AbstractNeuronEntity, String> filenameSelector,
                            Path outputDir) {
        String filename = filenameSelector.apply(groupedMatchedEntities.getKey());
        JsonOutputHelper.writeToJSONFile(
                groupedMatchedEntities,
                FSUtils.getOutputPath(outputDir, new File(filename) + ".json"),
                jsonWriter);
    }
}