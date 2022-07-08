package org.janelia.colormipsearch.cmd.io;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectWriter;

import org.janelia.colormipsearch.io.JsonOutputHelper;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDSMatch;
import org.janelia.colormipsearch.results.MatchResultsGrouping;
import org.janelia.colormipsearch.results.ResultMatches;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractJSONCDSWriter<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractJSONCDSWriter.class);

    protected final ObjectWriter jsonWriter;

    protected AbstractJSONCDSWriter(ObjectWriter jsonWriter) {
        this.jsonWriter = jsonWriter;
    }

    protected <M1 extends AbstractNeuronMetadata, T1 extends AbstractNeuronMetadata> void writeAllSearchResults(
            List<ResultMatches<M1, T1, CDSMatch<M1, T1>>> cdsMatchesList,
            Path outputDir) {
        long startTime = System.currentTimeMillis();
        IOUtils.createDirs(outputDir);
        LOG.info("Write {} file results to {}", cdsMatchesList.size(), outputDir);
        cdsMatchesList.stream().parallel()
                .forEach(cdsMatches -> writeSearchResults(cdsMatches, outputDir));
        LOG.info("Finished writing {} file results in {}s", cdsMatchesList.size(), (System.currentTimeMillis() - startTime) / 1000.);
    }

    private <M1 extends AbstractNeuronMetadata, T1 extends AbstractNeuronMetadata> void writeSearchResults(
            ResultMatches<M1, T1, CDSMatch<M1, T1>> cdsMatches,
            Path outputDir) {
        JsonOutputHelper.writeToJSONFile(
                cdsMatches,
                IOUtils.getOutputFile(outputDir, new File(cdsMatches.getKey().getId() + ".json")),
                jsonWriter);
    }
}