package org.janelia.colormipsearch.cmd;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.annotation.Nullable;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.dao.NeuronsMatchFilter;
import org.janelia.colormipsearch.dataio.DataSourceParam;
import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.dataio.NeuronMatchesWriter;
import org.janelia.colormipsearch.dataio.db.DBNeuronMatchesReader;
import org.janelia.colormipsearch.dataio.fs.JSONNeuronMatchesWriter;
import org.janelia.colormipsearch.dataio.fs.JSONNeuronMatchesReader;
import org.janelia.colormipsearch.datarequests.SortCriteria;
import org.janelia.colormipsearch.datarequests.SortDirection;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.model.PPPMatch;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This command is used to export data from the database to the file system in order to upload it to S3.
 */
public class ExportNeuronMatchesCmd extends AbstractCmd {

    private static final Logger LOG = LoggerFactory.getLogger(ExportNeuronMatchesCmd.class);

    @Parameters(commandDescription = "Color depth search for a batch of MIPs")
    static class ExportMatchesCmdArgs extends AbstractCmdArgs {

        @Parameter(names = {"--match-type"}, required = true, // this is required because PPPs are handled a bit differently
                description = "Specifies neuron match type whether it's color depth search, PPP, etc.")
        MatchResultTypes matchResultTypes = MatchResultTypes.CDS;

        @Parameter(names = {"--pctPositivePixels"}, description = "% of Positive PX Threshold (0-100%)")
        Double pctPositivePixels = 0.0;

        @Parameter(names = {"--with-grad-scores"},
                description = "Select matches with gradient scores",
                arity = 0)
        boolean withGradScores = false;

        @Parameter(names = {"--masks"}, description = "Masks library")
        String masksLibrary;

        @Parameter(names = {"--perMaskSubdir"}, description = "Results subdirectory for results grouped by mask MIP ID")
        String perMaskSubdir;

        @Parameter(names = {"--targets"}, description = "Targets library")
        String targetsLibrary;

        @Parameter(names = {"--perTargetSubdir"}, description = "Results subdirectory for results grouped by target MIP ID")
        String perTargetSubdir;

        @Parameter(names = {"--processingPartitionSize", "-ps", "--libraryPartitionSize"}, description = "Processing partition size")
        int processingPartitionSize = 100;

        public ExportMatchesCmdArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }

        @Nullable
        Path getPerMaskDir() {
            return getOutputDirArg()
                    .map(dir -> StringUtils.isNotBlank(perMaskSubdir) ? dir.resolve(perMaskSubdir) : dir)
                    .orElse(null);
        }

        @Nullable
        Path getPerTargetDir() {
            return getOutputDirArg()
                    .map(dir -> StringUtils.isNotBlank(perTargetSubdir) ? dir.resolve(perTargetSubdir) : dir)
                    .orElse(null);
        }
    }

    private final ExportMatchesCmdArgs args;
    private final ObjectMapper mapper;

    public ExportNeuronMatchesCmd(String commandName, CommonArgs commonArgs) {
        super(commandName);
        this.args = new ExportMatchesCmdArgs(commonArgs);
        this.mapper = new ObjectMapper()
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        ;
    }

    @Override
    ExportMatchesCmdArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        exportNeuronMatches();
    }

    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>>
    void exportNeuronMatches() {
        NeuronMatchesReader<M, T, R> neuronMatchesReader = getMatchesReader();
        NeuronMatchesWriter<M, T, R> perMaskNeuronMatchesWriter = getJSONMatchesWriter(args.getPerMaskDir(), null);
        NeuronMatchesWriter<M, T, R> perTargetNeuronMatchesWriter = getJSONMatchesWriter(null, args.getPerTargetDir());

        NeuronsMatchFilter<R> neuronsMatchFilter = new NeuronsMatchFilter<>();
        neuronsMatchFilter.setMatchType(args.matchResultTypes.getMatchType());
        if (args.pctPositivePixels > 0) {
            neuronsMatchFilter.addSScore("matchingPixelsRatio", args.pctPositivePixels / 100);
        }
        if (args.withGradScores) {
            neuronsMatchFilter.addSScore("gradientAreaGap", 0);
        }

        if (perMaskNeuronMatchesWriter != null)
            exportNeuronMatchesPerMask(neuronsMatchFilter, neuronMatchesReader, perMaskNeuronMatchesWriter);
        if (perTargetNeuronMatchesWriter != null)
            exportNeuronMatchesPerTarget(neuronsMatchFilter, neuronMatchesReader, perTargetNeuronMatchesWriter);
    }

    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>>
    void exportNeuronMatchesPerMask(NeuronsMatchFilter<R> neuronsMatchFilter,
                                    NeuronMatchesReader<M, T, R> neuronMatchesReader,
                                    NeuronMatchesWriter<M, T, R> perMaskNeuronMatchesWriter) {

        List<String> masks = neuronMatchesReader.listMatchesLocations(
                Collections.singletonList(new DataSourceParam(args.masksLibrary, 0, -1)));
        ItemsHandling.partitionCollection(masks, args.processingPartitionSize).stream().parallel()
                .forEach(partititionItems -> {
                    partititionItems.forEach(maskId -> {
                        LOG.info("Read color depth matches for {}", maskId);
                        List<R> matchesForMask = neuronMatchesReader.readMatchesForMasks(
                                null,
                                Collections.singletonList(maskId),
                                neuronsMatchFilter,
                                Collections.singletonList(
                                        new SortCriteria("normalizedScore", SortDirection.DESC)
                                ));
                        perMaskNeuronMatchesWriter.write(matchesForMask);
                    });
                });
    }

    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>>
    void exportNeuronMatchesPerTarget(NeuronsMatchFilter<R> neuronsMatchFilter,
                                      NeuronMatchesReader<M, T, R> neuronMatchesReader,
                                      NeuronMatchesWriter<M, T, R> perTargetNeuronMatchesWriter) {
        List<String> targets = neuronMatchesReader.listMatchesLocations(
                Collections.singletonList(new DataSourceParam(args.targetsLibrary, 0, -1)));
        ItemsHandling.partitionCollection(targets, args.processingPartitionSize).stream().parallel()
                .forEach(partititionItems -> {
                    partititionItems.forEach(targetId -> {
                        LOG.info("Read color depth matches for {}", targetId);
                        List<R> matchesForTarget = neuronMatchesReader.readMatchesForTargets(
                                null,
                                Collections.singletonList(targetId),
                                neuronsMatchFilter,
                                Collections.singletonList(
                                        new SortCriteria("normalizedScore", SortDirection.DESC)
                                ));
                    });
                });
    }

    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>>
    NeuronMatchesReader<M, T, R> getMatchesReader() {
        if (args.commonArgs.resultsStorage == StorageType.DB) {
            return new DBNeuronMatchesReader<>(getConfig());
        } else {
            return new JSONNeuronMatchesReader<>(mapper);
        }
    }

    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>>
    NeuronMatchesWriter<M, T, R> getJSONMatchesWriter(Path perMaskDir, Path perTargetDir) {
        if (perMaskDir != null || perTargetDir != null) {
            return new JSONNeuronMatchesWriter<>(
                    args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter(),
                    args.matchResultTypes.getMatchGrouping(),
                    args.matchResultTypes.getMatchOrdering(),
                    perMaskDir,
                    perTargetDir
            );
        } else
            return null;
    }

}
