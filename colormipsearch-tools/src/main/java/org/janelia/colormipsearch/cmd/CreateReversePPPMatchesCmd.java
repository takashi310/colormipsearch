package org.janelia.colormipsearch.cmd;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.api.Utils;

public class CreateReversePPPMatchesCmd extends AbstractCmd {

    @Parameters(commandDescription = "Create reverse PPP matches. Typically PPP matches are computed only from EM to LM, and " +
            "this provides a mechanism to artificially generate LM to EM PPP matches.")
    static class ReversePPPMatchesArgs extends AbstractCmdArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, converter = ListArg.ListArgConverter.class,
                description = "Results directory containing computed PPP matches")
        private ListArg resultsDir;

        @Parameter(names = {"--resultsFile", "-rf"}, variableArity = true, description = "File(s) containing computed PPP matches")
        private List<String> resultsFiles;

        @Parameter(names = {"--processingPartitionSize", "-ps"}, description = "Processing partition size")
        int processingPartitionSize = 100;

        @ParametersDelegate
        final CommonArgs commonArgs;

        ReversePPPMatchesArgs(CommonArgs commonArgs) {
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
                errors.add("No result file or directory containing PPP matches has been specified");
            }
            return errors;
        }
    }

    private final ReversePPPMatchesArgs args;

    CreateReversePPPMatchesCmd(String commandName, CommonArgs commonArgs) {
        super(commandName);
        this.args = new ReversePPPMatchesArgs(commonArgs);
    }

    @Override
    ReversePPPMatchesArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        CmdUtils.createOutputDirs(args.getOutputDir());
        createReversePPPMatches(args);
    }

    private void createReversePPPMatches(ReversePPPMatchesArgs args) {
        long startTime = System.currentTimeMillis();

        List<String> filesToProcess;
        if (CollectionUtils.isNotEmpty(args.resultsFiles)) {
            filesToProcess = args.resultsFiles;
        } else if (args.resultsDir != null) {
            filesToProcess = CmdUtils.getFileToProcessFromDir(args.resultsDir.input, args.resultsDir.offset, args.resultsDir.length);
        } else {
            filesToProcess = Collections.emptyList();
        }
        Path outputDir = args.getOutputDir();
        Utils.partitionCollection(filesToProcess, args.processingPartitionSize).stream().parallel()
                .forEach(fileList -> {
                    // TODO
                })
                ;
    }

}
