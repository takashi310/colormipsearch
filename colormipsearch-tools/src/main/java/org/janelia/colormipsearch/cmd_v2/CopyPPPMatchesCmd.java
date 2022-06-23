package org.janelia.colormipsearch.cmd_v2;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.janelia.colormipsearch.results.Results;
import org.janelia.colormipsearch.api_v2.Utils;
import org.janelia.colormipsearch.api_v2.pppsearch.AbstractPPPMatch;
import org.janelia.colormipsearch.api_v2.pppsearch.EmPPPMatch;
import org.janelia.colormipsearch.api_v2.pppsearch.EmPPPMatches;
import org.janelia.colormipsearch.api_v2.pppsearch.PPPUtils;
import org.janelia.colormipsearch.api_v2.pppsearch.PublishedEmPPPMatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CopyPPPMatchesCmd extends AbstractCmd {

    private static final Logger LOG = LoggerFactory.getLogger(CopyPPPMatchesCmd.class);

    @Parameters(commandDescription = "Copy PPP matches with options to clean up the data.")
    static class CopyPPPMatchesArgs extends AbstractCmdArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, converter = ListArg.ListArgConverter.class,
                description = "Results directory containing computed PPP matches")
        private ListArg resultsDir;

        @Parameter(names = {"--resultsFile", "-rf"}, variableArity = true, description = "File(s) containing computed PPP matches")
        private List<String> resultsFiles;

        @Parameter(names = {"--processingPartitionSize", "-ps"}, description = "Processing partition size")
        int processingPartitionSize = 100;

        @Parameter(names = {"--filterInternalFields"}, description = "Filter out internal fields such as sample name, etc.", arity = 0)
        boolean filterOutInternalFields;

        @Parameter(names = {"--truncatePartialResults"}, description = "Truncate partial results that do not have image files", arity = 0)
        boolean truncateResults;

        @Parameter(names = {"--emDatasetMapping"}, description = "EM library name")
        String emDatasetMapping;

        @Parameter(names = {"--lmDatasetMapping"}, description = "EM library name")
        String lmDatasetMapping;

        @ParametersDelegate
        final CommonArgs commonArgs;

        CopyPPPMatchesArgs(CommonArgs commonArgs) {
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

    private final CopyPPPMatchesArgs args;
    private final ObjectMapper mapper;

    CopyPPPMatchesCmd(String commandName, CommonArgs commonArgs) {
        super(commandName);
        this.args = new CopyPPPMatchesArgs(commonArgs);
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    CopyPPPMatchesArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        CmdUtils.createOutputDirs(args.getOutputDir());
        copyPPPMatches(args);
    }

    private void copyPPPMatches(CopyPPPMatchesArgs args) {
        List<String> filesToProcess;
        if (CollectionUtils.isNotEmpty(args.resultsFiles)) {
            filesToProcess = args.resultsFiles;
        } else if (args.resultsDir != null) {
            filesToProcess = CmdUtils.getFileToProcessFromDir(args.resultsDir.input, args.resultsDir.offset, args.resultsDir.length);
        } else {
            filesToProcess = Collections.emptyList();
        }
        Path outputDir = args.getOutputDir();
        ItemsHandling.partitionCollection(filesToProcess, args.processingPartitionSize).stream().parallel()
                .flatMap(fileList -> fileList.stream()
                        .map(f -> PPPUtils.readEmPPPMatchesFromJSONFile(new File(f), mapper))
                        .filter(Results::hasResults)
                        .map(res -> {
                            List<EmPPPMatch> pppMatches = res.getResults().stream()
                                            .filter(r -> !args.truncateResults || r.hasSourceImageFiles())
                                            .map(r -> args.filterOutInternalFields ? PublishedEmPPPMatch.createReleaseCopy(r) : r)
                                            .map(r -> new AbstractPPPMatch.Update<>(r)
                                                    .applyUpdate((pppMatch, v) -> {
                                                        if (StringUtils.isNotBlank(v)) {
                                                            pppMatch.setSourceEmDataset(v);
                                                        }
                                                    }, args.emDatasetMapping)
                                                    .applyUpdate((pppMatch, v) -> {
                                                        if (StringUtils.isNotBlank(v)) {
                                                            pppMatch.setSourceLmDataset(v);
                                                        }
                                                    }, args.lmDatasetMapping)
                                                    .get()
                                            )
                                            .collect(Collectors.toList());
                            if (pppMatches.isEmpty()) {
                                LOG.info("No valid PPP matches exist for {}", res.getNeuronName());
                            }
                            return EmPPPMatches.pppMatchesBySingleNeuron(pppMatches);
                        }))
                .filter(Results::hasResults)
                .forEach(res -> {
                    Utils.writeResultsToJSONFile(
                            res,
                            CmdUtils.getOutputFile(outputDir, new File(res.getNeuronName() + ".json")),
                            args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter());
                })
                ;
    }

}
