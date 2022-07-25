package org.janelia.colormipsearch.cmd;

import java.util.Comparator;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.janelia.colormipsearch.cmd.v2dataimport.JSONV2Em2LmMatchesReader;
import org.janelia.colormipsearch.dataio.NeuronMatchesReader;
import org.janelia.colormipsearch.dataio.NeuronMatchesWriter;
import org.janelia.colormipsearch.dataio.PartitionedNeuronMatchesWriter;
import org.janelia.colormipsearch.dataio.db.DBCDScoresOnlyWriter;
import org.janelia.colormipsearch.dataio.db.DBNeuronMatchesWriter;
import org.janelia.colormipsearch.dataio.fs.JSONNeuronMatchesWriter;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDMatch;
import org.janelia.colormipsearch.model.EMNeuronMetadata;
import org.janelia.colormipsearch.model.LMNeuronMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This command is used to export data from the database to the file system in order to upload it to S3.
 */
public class ImportV2CDMatchesCmd extends AbstractCmd {

    private static final Logger LOG = LoggerFactory.getLogger(ImportV2CDMatchesCmd.class);

    @Parameters(commandDescription = "Import v2 color depth matches")
    static class ImportCDMatchesCmdArgs extends AbstractCmdArgs {

        @Parameter(names = {"--results", "-r"}, required = true, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "The location of the v2 results. This can be a list of directories or files ")
        List<ListArg> matchesResults;

        public ImportCDMatchesCmdArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }

    }

    private final ImportCDMatchesCmdArgs args;
    private final ObjectMapper mapper;

    public ImportV2CDMatchesCmd(String commandName, CommonArgs commonArgs) {
        super(commandName);
        this.args = new ImportCDMatchesCmdArgs(commonArgs);
        this.mapper = new ObjectMapper()
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        ;
    }

    @Override
    public ImportCDMatchesCmdArgs getArgs() {
        return args;
    }

    @Override
    public void execute() {
        NeuronMatchesReader<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> cdMatchesReader = getCDMatchesReader();
        NeuronMatchesWriter<EMNeuronMetadata, LMNeuronMetadata, CDMatch<EMNeuronMetadata, LMNeuronMetadata>> cdMatchesWriter = getCDSMatchesWriter();
        // TODO
    }

    private NeuronMatchesReader<CDMatch<EMNeuronMetadata, LMNeuronMetadata>> getCDMatchesReader() {
        return new JSONV2Em2LmMatchesReader(mapper);
    }

    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> NeuronMatchesWriter<M, T, CDMatch<M, T>>
    getCDSMatchesWriter() {
        if (args.commonArgs.resultsStorage == StorageType.DB) {
            // always create new matches
            return new DBNeuronMatchesWriter<>(getConfig());
        } else {
            throw new IllegalArgumentException("This class should only be used for importing intermediated results into the database");
        }
    }

}
