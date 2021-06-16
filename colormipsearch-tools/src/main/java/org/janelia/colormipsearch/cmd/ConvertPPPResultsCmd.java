package org.janelia.colormipsearch.cmd;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class ConvertPPPResultsCmd extends AbstractCmd {

    @Parameters(commandDescription = "Convert the original PPP results into NeuronBridge compatible results")
    static class ConvertPPPResultsArgs extends AbstractCmdArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, converter = ListArg.ListArgConverter.class,
                description = "Location of the original PPP results")
        private ListArg resultsDir;

        @Parameter(names = {"--resultsFile", "-rf"}, variableArity = true,
                description = "File(s) containing original PPP results")
        private List<String> resultsFiles;

        @ParametersDelegate
        final CommonArgs commonArgs;

        ConvertPPPResultsArgs(CommonArgs commonArgs) {
            this.commonArgs = commonArgs;
        }

        Path getOutputDir() {
            if (StringUtils.isNotBlank(commonArgs.outputDir)) {
                return Paths.get(commonArgs.outputDir);
            } else {
                return null;
            }
        }

        @Override
        List<String> validate() {
            List<String> errors = new ArrayList<>();
            boolean inputFound = resultsDir != null || CollectionUtils.isNotEmpty(resultsFiles);
            if (!inputFound) {
                errors.add("No result file(s) or directory containing original PPP results has been specified");
            }
            return errors;
        }
    }

    private final ObjectMapper mapper;
    private final ConvertPPPResultsArgs args;

    ConvertPPPResultsCmd(String commandName, CommonArgs commonArgs) {
        super(commandName);
        this.args = new ConvertPPPResultsArgs(commonArgs);
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    AbstractCmdArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        CmdUtils.createOutputDirs(args.getOutputDir());
        convertPPPResults(args);
    }

    private void convertPPPResults(ConvertPPPResultsArgs args) {
        long startTime = System.currentTimeMillis();

        List<String> filesToProcess;
        if (CollectionUtils.isNotEmpty(args.resultsFiles)) {
            filesToProcess = args.resultsFiles;
        } else if (args.resultsDir != null) {
        } else {
            filesToProcess = Collections.emptyList();
        }
    }

}
