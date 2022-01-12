package org.janelia.colormipsearch.cmd;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.api.Results;
import org.janelia.colormipsearch.api.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MergeMipsCmd extends AbstractCmd {
    private static final Logger LOG = LoggerFactory.getLogger(MergeMipsCmd.class);

    @Parameters(commandDescription = "Merge color depth search results")
    static class MergeMipsArgs extends AbstractCmdArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, variableArity = true, description = "Results directory to be combined")
        List<String> resultsDirs;

        @Parameter(names = {"--resultsFile", "-rf"}, variableArity = true, description = "File containing results to be combined")
        List<String> resultsFiles;

        @ParametersDelegate
        final CommonArgs commonArgs;

        MergeMipsArgs(CommonArgs commonArgs) {
            this.commonArgs = commonArgs;
        }

        Path getOutputDir() {
            if (StringUtils.isNotBlank(commonArgs.outputDir)) {
                return Paths.get(commonArgs.outputDir);
            } else {
                return null;
            }
        }
    }

    private final MergeMipsArgs args;
    private final ObjectMapper mapper;

    MergeMipsCmd(String commandName, CommonArgs commonArgs) {
        super(commandName);
        args =  new MergeMipsArgs(commonArgs);
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    MergeMipsArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        CmdUtils.createOutputDirs(args.getOutputDir());
        mergeResults(args);
    }

    private void mergeResults(MergeMipsArgs args) {
        List<String> resultFileNames;
        if (CollectionUtils.isNotEmpty(args.resultsFiles)) {
            resultFileNames = args.resultsFiles;
        } else if (CollectionUtils.isNotEmpty(args.resultsDirs)) {
            resultFileNames = args.resultsDirs.stream()
                    .flatMap(rd -> {
                        try {
                            return Files.find(Paths.get(rd), 1, (p, fa) -> fa.isRegularFile());
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .map(p -> p.toString())
                    .collect(Collectors.toList());
        } else {
            resultFileNames = Collections.emptyList();
        }
        mergeMipsFiles(resultFileNames, args.getOutputDir());
    }

    private void mergeMipsFiles(List<String> inputResultsFilenames, Path outputDir) {
        // files that have the same file name (but coming from different directories)
        // will be combined in a single result file.
        Map<String, List<String>> resultFilesToCombinedTogether = inputResultsFilenames.stream()
                .collect(Collectors.groupingBy(fn -> Paths.get(fn).getFileName().toString(), Collectors.toList()));

        resultFilesToCombinedTogether.entrySet().stream().parallel()
                .forEach(e -> {
                    String fn = e.getKey();
                    List<String> resultList = e.getValue();
                    LOG.info("Combine results for {} from {}", fn, resultList);
                    List<ColorDepthMetadata> combinedResults = resultList.stream()
                            .map(File::new)
                            .map(mipsFile -> {
                                LOG.info("Reading {} -> {}", fn, mipsFile);
                                Results<List<ColorDepthMetadata>> mipsResults = readMipsMetadata(mipsFile);
                                if (!mipsResults.hasResults()) {
                                    LOG.warn("Results file {} is empty", mipsFile);
                                }
                                return mipsResults;
                            })
                            .filter(mipsResults -> CollectionUtils.isNotEmpty(mipsResults.results))
                            .flatMap(mipsResults -> mipsResults.results.stream())
                            .collect(Collectors.toList());
                    Utils.writeResultsToJSONFile(
                            new Results<>(combinedResults),
                            CmdUtils.getOutputFile(outputDir, new File(fn)),
                            args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter()
                    );
                });
    }

    private Results<List<ColorDepthMetadata>> readMipsMetadata(File mipsFile) {
        try {
            return mapper.readValue(mipsFile, new TypeReference<Results<List<ColorDepthMetadata>>>() {});
        } catch (Exception e) {
            LOG.error("Error reading MIPs from json file {}", mipsFile, e);
            throw new IllegalStateException(e);
        }
    }

    public static <T, R extends Results<List<T>>> void writeResultsToJSONFile(R results, File f, ObjectWriter objectWriter) {
        try {
            if (CollectionUtils.isNotEmpty(results.getResults())) {
                if (f == null) {
                    objectWriter.writeValue(System.out, results);
                } else {
                    LOG.info("Writing {}", f);
                    objectWriter.writeValue(f, results);
                }
            }
        } catch (IOException e) {
            LOG.error("Error writing CDS results to json file {}", f, e);
            throw new UncheckedIOException(e);
        }
    }

}
