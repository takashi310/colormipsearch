package org.janelia.colormipsearch.cmd;

import java.nio.file.Path;

import javax.annotation.Nullable;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.dao.NeuronsMatchFilter;
import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.dataio.NeuronMatchesWriter;
import org.janelia.colormipsearch.dataio.db.DBNeuronMatchesReader;
import org.janelia.colormipsearch.dataio.fs.JSONCDSMatchesWriter;
import org.janelia.colormipsearch.dataio.fs.JSONNeuronMatchesReader;
import org.janelia.colormipsearch.dataio.fs.JSONPPPMatchesWriter;
import org.janelia.colormipsearch.model.AbstractMatch;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        @Parameter(names = {"--perMaskSubdir"}, description = "Results subdirectory for results grouped by mask MIP ID")
        String perMaskSubdir;

        @Parameter(names = {"--perTargetSubdir"}, description = "Results subdirectory for results grouped by target MIP ID")
        String perTargetSubdir;

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
        NeuronMatchesWriter<M, T, R> neuronMatchesWriter = getMatchesWriter();

        NeuronsMatchFilter<CDMatch<M, T>> neuronsMatchFilter = new NeuronsMatchFilter<>();
        neuronsMatchFilter.setMatchType(args.matchResultTypes.getMatchType());
        if (args.pctPositivePixels > 0) {
            neuronsMatchFilter.addSScore("matchingPixelsRatio", args.pctPositivePixels / 100);
        }
        if (args.withGradScores) {
            neuronsMatchFilter.addSScore("gradientAreaGap", 0);
        }

    }

    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>>
    NeuronMatchesReader<M, T, R> getMatchesReader() {
        if (args.commonArgs.resultsStorage == StorageType.DB) {
            return new DBNeuronMatchesReader<>(getConfig());
        } else {
            return new JSONNeuronMatchesReader<>(mapper);
        }
    }

    @SuppressWarnings("unchecked")
    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata, R extends AbstractMatch<M, T>>
    NeuronMatchesWriter<M, T, R> getMatchesWriter() {
        if (args.matchResultTypes == MatchResultTypes.CDS) {
            return (NeuronMatchesWriter<M, T, R>) new JSONCDSMatchesWriter<>(
                    args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter(),
                    args.getPerMaskDir(),
                    args.getPerTargetDir()
            );
        } else if (args.matchResultTypes == MatchResultTypes.PPP) {
            return (NeuronMatchesWriter<M, T, R>) new JSONPPPMatchesWriter<>(
                    args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter(),
                    args.getOutputDir()
            );
        } else {
            throw new IllegalArgumentException("Invalid match result type: " + args.matchResultTypes);
        }
    }

}
