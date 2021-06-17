package org.janelia.colormipsearch.cmd;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.colormipsearch.api.pppsearch.PPPMatch;
import org.janelia.colormipsearch.api.pppsearch.PPPMatches;
import org.janelia.colormipsearch.api.pppsearch.PPPUtils;
import org.janelia.colormipsearch.api.pppsearch.RawPPPMatchesReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertPPPResultsCmd extends AbstractCmd {
    private static final Logger LOG = LoggerFactory.getLogger(ConvertPPPResultsCmd.class);

    @Parameters(commandDescription = "Convert the original PPP results into NeuronBridge compatible results")
    static class ConvertPPPResultsArgs extends AbstractCmdArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, converter = ListArg.ListArgConverter.class,
                description = "Location of the original PPP results")
        private ListArg resultsDir;

        @Parameter(names = {"--resultsFile", "-rf"}, variableArity = true,
                description = "File(s) containing original PPP results. As a note these can be either ")
        private List<String> resultsFiles;

        @Parameter(names = "-neuronMatchesSubDir", description = "The name of the sub-directory containing the results")
        private String neuronMatchesSubDirName = "lm_cable_length_20_v4_adj_by_cov_numba_agglo_aT";

        @Parameter(names = "--jsonMatchesPrefix", description = "The prefix of the JSON results file containing PPP matches")
        private String jsonPPPResultsPrefix = "cov_scores_";

        @Parameter(names = {"--processingPartitionSize", "-ps"}, description = "Processing partition size")
        int processingPartitionSize = 100;

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
    private final RawPPPMatchesReader originalPPPMatchesReader;

    ConvertPPPResultsCmd(String commandName, CommonArgs commonArgs) {
        super(commandName);
        this.args = new ConvertPPPResultsArgs(commonArgs);
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.originalPPPMatchesReader = new RawPPPMatchesReader();
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
        } else {
            filesToProcess = Collections.emptyList();
        }
        Collection<PPPMatches> pppMatchesCollection = importPPPResults(filesToProcess.stream().map(fn -> new File(fn)).collect(Collectors.toList()));
        pppMatchesCollection.forEach(pppMatches -> PPPUtils.writePPPMatchesToJSONFile(
                pppMatches,
                null,
                args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter()));
    }

    private List<PPPMatches> importPPPMatchesFromResultsPath(Path pppResultsForNeuronPath) {
        if (Files.isDirectory(pppResultsForNeuronPath)) {
            try {
                Files.list(pppResultsForNeuronPath)
                        .collect(Collectors.toList());
            } catch (IOException e) {
                LOG.error("Error traversing {}", pppResultsForNeuronPath, e);
            }
            return Collections.emptyList(); // !!!!!!!!!
//        } else if (Files.isRegularFile(pppResultsForNeuronPath, LinkOption.NOFOLLOW_LINKS)) {
//            return PPPMatches.fromListOfPPPMatches(importPPPResults(pppResultsForNeuronPath.toFile()));
        } else {
            LOG.error("Links are not supported");
            return Collections.emptyList();
        }
    }

    private Collection<PPPMatches> importPPPResults(List<File> pppResultsFiles) {
        return pppResultsFiles.stream()
                .collect(Collectors.groupingBy(
                        f -> f.getName()
                                .replaceAll("(_\\d+)?\\.json$", "")
                                .replaceAll(args.jsonPPPResultsPrefix, ""),
                        Collectors.collectingAndThen(Collectors.toList(),
                                listOfMatches -> {
                                    List<PPPMatch> neuronMatches = PPPUtils.mergeMatches(
                                            listOfMatches.stream()
                                                    .flatMap(f -> originalPPPMatchesReader.readPPPMatches(f).stream())
                                                    .collect(Collectors.toList()),
                                            Collections.emptyList());
                                    return PPPMatches.pppMatchesBySingleNeuron(neuronMatches);
                                })))
                .values()
                ;
    }
}
