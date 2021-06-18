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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import org.apache.commons.math3.geometry.spherical.oned.ArcsSet;
import org.janelia.colormipsearch.api.Utils;
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
            if (processingPartitionSize <= 0) {
                errors.add("Processing partition size must be greater than 0");
            }
            return errors;
        }
    }

    private static class RangeSpliterator<T> implements Spliterator<T> {
        private AtomicLong index;
        private final long from;
        private final int length;
        private final long to;
        private final Spliterator<T> wrapped;

        RangeSpliterator(Spliterator<T> wrapped, AtomicLong index, long from, int length) {
            this.wrapped = wrapped;
            this.index = index;
            this.from = Math.max(from, 0);
            this.length = length;
            this.to = length > 0 ? this.from + length : -1;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            if (wrapped == null) {
                return false;
            } else {
                long currentIndex = index.getAndIncrement();
                boolean hasNext = wrapped.tryAdvance(e -> {
                    if (currentIndex >= from && (to < 0 || currentIndex < to)) {
                        action.accept(e);
                    }
                });
                return hasNext && (to < 0 || currentIndex < to);
            }
        }

        @Override
        public Spliterator<T> trySplit() {
            if (wrapped == null) {
                return null;
            } else {
                return new RangeSpliterator<>(wrapped.trySplit(), index, from, length);
            }
        }

        @Override
        public long estimateSize() {
            return wrapped != null ? wrapped.estimateSize() : 0;
        }

        @Override
        public int characteristics() {
            return wrapped != null ? wrapped.characteristics() : 0;
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
        Stream<List<Path>> filesToProcess;
        if (CollectionUtils.isNotEmpty(args.resultsFiles)) {
            filesToProcess = groupFilesByNeuron(
                    args.resultsFiles.stream().map(fn -> Paths.get(fn)).collect(Collectors.toList())
            ).stream();
        } else {
            Stream<Path> allDirsWithPPPResults = streamDirsWithPPPResults(args.resultsDir.getInputPath());
            Stream<Path> dirsToProcess;
            int offset = Math.max(0, args.resultsDir.offset);
            if (args.resultsDir.length > 0) {
                dirsToProcess = allDirsWithPPPResults.skip(offset).limit(args.resultsDir.length);
            } else {
                dirsToProcess = allDirsWithPPPResults.skip(offset);
            }
            filesToProcess = dirsToProcess.map(d -> getPPPResultsFromDir(d));
        }
        LOG.info("Got all files to process after {}s", (System.currentTimeMillis()-startTime)/1000.);
        Utils.partitionStream(filesToProcess, args.processingPartitionSize).stream().parallel()
                .forEach(this::processListOfNeuronResults);
        LOG.info("Processed all files in {}s", (System.currentTimeMillis()-startTime)/1000.);
    }

    private void processListOfNeuronResults(List<List<Path>> listOfPPPResults) {
        long start = System.currentTimeMillis();
        listOfPPPResults.stream()
                .map(files -> importPPPRResultsFromFiles(files))
                .forEach(pppMatches -> PPPUtils.writePPPMatchesToJSONFile(
                        pppMatches,
                        null,
                        args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter()));
        LOG.info("Processed {} PPP results in {}s", listOfPPPResults.size(), (System.currentTimeMillis()-start)/1000.);
    }

    private Stream<Path> streamDirsWithPPPResults(Path startPath) {
        try {
            if (startPath.getFileName().toString().equals(args.neuronMatchesSubDirName)) {
                return Stream.of(startPath);
            } else {
                return Files.list(startPath)
                        .filter(Files::isDirectory)
                        .filter(p -> !p.getFileName().toString().startsWith("nblastScores")) // do not go into nblastScores dirs
                        .filter(p -> !p.getFileName().toString().startsWith("screenshots")) // do not go into screenshots dirs
                        .flatMap(p -> streamDirsWithPPPResults(p))
                        ;
            }
        } catch (IOException e) {
            LOG.error("Error traversing {}", startPath, e);
            return Stream.empty();
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
                        .peek(pppMatch -> fillInPPPMetadata(pppMatch))
                        .collect(Collectors.toList()),
                Collections.emptyList());
        return PPPMatches.pppMatchesBySingleNeuron(neuronMatches);
    }

    private void fillInPPPMetadata(PPPMatch pppMatch) {
        fillEMMMetadata(pppMatch.getFullEmName(), pppMatch);
        fillLMMetadata(pppMatch.getFullLmName(), pppMatch);
    }

    private void fillEMMMetadata(String emFullName, PPPMatch pppMatch) {
        Pattern emRegExPattern = Pattern.compile("([0-9]+)-([^-]*)-(.*)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = emRegExPattern.matcher(emFullName);
        if (matcher.find()) {
            pppMatch.setNeuronName(matcher.group(1));
            pppMatch.setNeuronType(matcher.group(2));
        }
    }

    private void fillLMMetadata(String lmFullName, PPPMatch pppMatch) {
        Pattern lmRegExPattern = Pattern.compile("(.+)_REG_UNISEX_(.+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = lmRegExPattern.matcher(lmFullName);
        if (matcher.find()) {
            pppMatch.setLmSampleName(matcher.group(1));
            pppMatch.setObjective(matcher.group(2));
        }
    }

}
