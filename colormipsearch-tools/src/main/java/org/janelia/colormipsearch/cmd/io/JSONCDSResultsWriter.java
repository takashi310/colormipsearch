package org.janelia.colormipsearch.cmd.io;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.ImmutableList;

import org.janelia.colormipsearch.io.JsonOutputHelper;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDSMatch;
import org.janelia.colormipsearch.results.MatchResultsGrouping;
import org.janelia.colormipsearch.results.ResultMatches;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONCDSResultsWriter<M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> implements ResultMatchesWriter<M, T, CDSMatch<M, T>> {
    private static final Logger LOG = LoggerFactory.getLogger(JSONCDSResultsWriter.class);

    private final ObjectWriter jsonWriter;
    private final Path perMasksOutputDir;
    private final Path perMatchesOutputDir;

    public JSONCDSResultsWriter(ObjectWriter jsonWriter,
                                Path perMasksOutputDir,
                                Path perMatchesOutputDir) {
        this.jsonWriter = jsonWriter;
        this.perMatchesOutputDir = perMatchesOutputDir;
        this.perMasksOutputDir = perMasksOutputDir;
    }

    public void write(List<CDSMatch<M, T>> cdsMatches) {
        // write results by mask ID (creating the collection right before it's passed as and arg in order to type match)
        writeAllSearchResults(
                MatchResultsGrouping.groupByMaskFields(
                        cdsMatches,
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
                        cdsMatches,
                        Collections.singletonList(
                                AbstractNeuronMetadata::getId
                        ),
                        Comparator.comparingDouble(m -> -m.getMatchingPixels())
                ),
                perMatchesOutputDir
        );
        try {
            cdsMatches.sort((m1, m2) -> -m1.getMatchingPixels());
            jsonWriter.writeValue(perMasksOutputDir.getParent().resolve("allmatches.json").toFile(), cdsMatches);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private <M1 extends AbstractNeuronMetadata, T1 extends AbstractNeuronMetadata> void writeAllSearchResults(
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