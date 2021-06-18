package org.janelia.colormipsearch.cmd;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
        Collection<List<Path>> filesToProcess;
        if (CollectionUtils.isNotEmpty(args.resultsFiles)) {
            filesToProcess = groupFilesByNeuron(args.resultsFiles.stream().map(fn -> Paths.get(fn)).collect(Collectors.toList()));
        } else {
            List<Path> allDirsWithPPPResults = listDirsWithPPPResults(args.resultsDir.getInputPath());
            int from = Math.max(args.resultsDir.offset, 0);
            int to = args.resultsDir.length > 0
                    ? Math.min(from + args.resultsDir.length, allDirsWithPPPResults.size())
                    : allDirsWithPPPResults.size();
            List<Path> dirsToProcess = from > 0 || to < allDirsWithPPPResults.size()
                    ? allDirsWithPPPResults.subList(from, to)
                    : allDirsWithPPPResults;
            filesToProcess = dirsToProcess.stream().parallel()
                    .map(d -> getPPPResultsFromDir(d))
                    .collect(Collectors.toList());
        }
        filesToProcess.stream()
                .map(files -> importPPPRResultsFromFiles(files))
                .forEach(pppMatches -> PPPUtils.writePPPMatchesToJSONFile(
                        pppMatches,
                        null,
                        args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter()));
    }

    private List<Path> listDirsWithPPPResults(Path startPath) {
        try {
            if (startPath.getFileName().toString().equals(args.neuronMatchesSubDirName)) {
                return Collections.singletonList(startPath);
            } else {
                return Files.list(startPath).parallel()
                        .filter(Files::isDirectory)
                        .filter(p -> !p.getFileName().toString().startsWith("nblastScores")) // do not go into nblastScores dirs
                        .filter(p -> !p.getFileName().toString().startsWith("screenshots")) // do not go into screenshots dirs
                        .flatMap(p -> listDirsWithPPPResults(p).stream())
                        .collect(Collectors.toList());
            }
        } catch (IOException e) {
            LOG.error("Error traversing {}", startPath, e);
            return Collections.emptyList();
        }
    }

    private List<Path> getPPPResultsFromDir(Path pppResultsDir) {
        try {
            return Files.list(pppResultsDir)
                    .filter(Files::isRegularFile)
                    .filter(p -> {
                        String fn = p.getFileName().toString();
                        return fn.startsWith(args.jsonPPPResultsPrefix) && fn.endsWith(".json");
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            LOG.error("Error getting PPP JSON result file names from {}", pppResultsDir, e);
            return Collections.emptyList();
        }
    }

    private Collection<List<Path>> groupFilesByNeuron(List<Path> pppResultFiles) {
        return pppResultFiles.stream()
                .collect(Collectors.groupingBy(
                        f -> f.getFileName().toString()
                                .replaceAll("(_\\d+)?\\.json$", "")
                                .replaceAll(args.jsonPPPResultsPrefix, ""),
                        Collectors.toList()))
                .values();
    }

    /**
     * Import PPP results from a list of file matches that are all for the same neuron.
     *
     * @param pppResultsForSameNeuronFiles
     * @return
     */
    private PPPMatches importPPPRResultsFromFiles(List<Path> pppResultsForSameNeuronFiles) {
        List<PPPMatch> neuronMatches = PPPUtils.mergeMatches(
                pppResultsForSameNeuronFiles.stream()
                        .flatMap(f -> originalPPPMatchesReader.readPPPMatches(f.toFile()).stream())
                        .collect(Collectors.toList()),
                Collections.emptyList());
        return PPPMatches.pppMatchesBySingleNeuron(neuronMatches);
    }

}
