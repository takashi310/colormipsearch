package org.janelia.colormipsearch.cmd;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import org.janelia.colormipsearch.api.GradientAreaGapUtils;
import org.janelia.colormipsearch.tools.ColorMIPSearchResultMetadata;
import org.janelia.colormipsearch.tools.ColorMIPSearchResultUtils;
import org.janelia.colormipsearch.tools.Results;
import org.janelia.colormipsearch.tools.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RenameLibraryNameCmd {
    private static final Logger LOG = LoggerFactory.getLogger(RenameLibraryNameCmd.class);

    @Parameters(commandDescription = "Rename library")
    static class RenameLibraryArgs {
        @Parameter(names = {"--resultsDir", "-rd"}, converter = ListArg.ListArgConverter.class,
                description = "Results directory for which scores will be normalized")
        private ListArg resultsDir;

        @Parameter(names = {"--resultsFile", "-rf"}, variableArity = true,
                description = "Files for which results will be normalized")
        private List<String> resultsFiles;

        @Parameter(names = "-old-libname", description = "old library name", required = true)
        private String oldLibName;

        @Parameter(names = "-new-libname", description = "new library name", required = true)
        private String newLibName;

        @Parameter(names = "-no-pretty-print", description = "Do not pretty print the results", arity = 0)
        private boolean noPrettyPrint = false;

        @ParametersDelegate
        final CommonArgs commonArgs;

        RenameLibraryArgs(CommonArgs commonArgs) {
            this.commonArgs = commonArgs;
        }

        Path getOutputDir() {
            if (StringUtils.isNotBlank(commonArgs.outputDir)) {
                return Paths.get(commonArgs.outputDir);
            } else {
                return null;
            }
        }

        boolean validate() {
            return resultsDir != null || CollectionUtils.isNotEmpty(resultsFiles);
        }
    }

    private final RenameLibraryArgs args;
    private final ObjectMapper mapper;

    RenameLibraryNameCmd(CommonArgs commonArgs) {
        args = new RenameLibraryArgs(commonArgs);
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    RenameLibraryArgs getArgs() {
        return args;
    }

    void execute() {
        renameLibrary(args);
    }

    private void renameLibrary(RenameLibraryArgs args) {
        List<String> filesToProcess;
        if (CollectionUtils.isNotEmpty(args.resultsFiles)) {
            filesToProcess = args.resultsFiles;
        } else if (args.resultsDir != null) {
            try {
                int from = Math.max(args.resultsDir.offset, 0);
                int length = args.resultsDir.length;
                List<String> filenamesList = Files.find(Paths.get(args.resultsDir.input), 1, (p, fa) -> fa.isRegularFile())
                        .skip(from)
                        .map(Path::toString)
                        .collect(Collectors.toList());
                if (length > 0 && length < filenamesList.size()) {
                    filesToProcess = filenamesList.subList(0, length);
                } else {
                    filesToProcess = filenamesList;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            filesToProcess = Collections.emptyList();
        }
        filesToProcess.stream().parallel().forEach((fn) -> {
            LOG.info("Replace library name for {}", fn);
            File cdsFile = new File(fn);
            Results<List<ColorMIPSearchResultMetadata>> resultsFromJSONFile = ColorMIPSearchResultUtils.readCDSResultsFromJSONFile(cdsFile, mapper);
            List<ColorMIPSearchResultMetadata> cdsResults = resultsFromJSONFile.results;
            List<ColorMIPSearchResultMetadata> cdsResultsWithNormalizedScore = cdsResults.stream()
                    .peek(csr -> {
                        if (args.oldLibName.equals(csr.getLibraryName())) {
                            csr.setLibraryName(args.newLibName);
                        } else if (args.oldLibName.equals(csr.getAttr("Library"))) {
                            csr.addAttr("Library", args.newLibName);
                        }
                    })
                    .collect(Collectors.toList());
            ColorMIPSearchResultUtils.sortCDSResults(cdsResultsWithNormalizedScore);
            ColorMIPSearchResultUtils.writeCDSResultsToJSONFile(new Results<>(cdsResultsWithNormalizedScore), CmdUtils.getOutputFile(args.getOutputDir(), new File(fn)),
                    args.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter());
        });
    }

}
