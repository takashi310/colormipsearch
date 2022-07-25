package org.janelia.colormipsearch.cmd;

import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This command is used to export data from the database to the file system in order to upload it to S3.
 */
public class ImportV2CDMatchesCmd extends AbstractCmd {

    private static final Logger LOG = LoggerFactory.getLogger(ImportV2CDMatchesCmd.class);

    @Parameters(commandDescription = "Import v2 color depth matches")
    static class ImportCDMatchesCmdArgs extends AbstractCmdArgs {

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
        // TODO
    }

}
