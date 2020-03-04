package org.janelia.colormipsearch;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static class MainArgs {
        @Parameter(names = "-h", description = "Display the help message", help = true, arity = 0)
        private boolean displayHelpMessage = false;
    }

    private static class AbstractArgs {
        @Parameter(names = "--app")
        String appName = "ColorMIPSearch";

        @Parameter(names = {"--dataThreshold"}, description = "Data threshold")
        Integer dataThreshold = 100;

        @Parameter(names = {"--maskThreshold"}, description = "Mask threshold")
        Integer maskThreshold = 100;

        @Parameter(names = {"--pixColorFluctuation"}, description = "Pix Color Fluctuation, 1.18 per slice")
        Double pixColorFluctuation = 2.0;

        @Parameter(names = {"--xyShift"}, description = "Number of pixels to try shifting in XY plane")
        Integer xyShift = 0;

        @Parameter(names = {"--mirrorMask"}, description = "Should the mask be mirrored across the Y axis?")
        boolean mirrorMask = false;

        @Parameter(names = {"--pctPositivePixels"}, description = "% of Positive PX Threshold (0-100%)")
        Double pctPositivePixels = 0.0;

        @Parameter(names = {"--outputDir", "-od"}, description = "Output directory")
        String outputDir;

        @Parameter(names = {"--gradientDir", "-gd"}, description = "Gradient masks directory")
        String gradientDir;

        @Parameter(names = {"--hemiMask"}, description = "Hemibrain mask")
        String hemiMask = "MAX_hemi_to_JRC2018U_fineTune.png";

        @Parameter(names = "-cdsConcurrency", description = "CDS concurrency - number of CDS tasks run concurrently")
        int cdsConcurrency = 100;

        @Parameter(names = "-h", description = "Display the help message", help = true, arity = 0)
        boolean displayHelpMessage = false;
    }

    @Parameters(commandDescription = "Single color depth search")
    private static class SingleSearchArgs extends AbstractArgs {
        @Parameter(names = "-i", required = true, description = "Library MIP path")
        private String libraryPath;

        @Parameter(names = "-m", required = true, description = "Mask path")
        private String maskPath;

        @Parameter(names = "-result", description = "Result file name")
        private String resultName;
    }

    @Parameters(commandDescription = "Batch color depth search")
    private static class BatchSearchArgs extends AbstractArgs {
        @Parameter(names = {"--images", "-i"}, required = true, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "Comma-delimited list of directories containing images to search")
        private List<ListArg> librariesInputs;

        @Parameter(names = {"--masks", "-m"}, required = true, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "Image file(s) to use as the search masks")
        private List<ListArg> masksInputs;

        @Parameter(names = "-locally", description = "Perform the search in the current process", arity = 0)
        private boolean useLocalProcessing = false;
    }

    public static void main(String[] argv) {
        MainArgs mainArgs = new MainArgs();
        BatchSearchArgs batchSearchArgs = new BatchSearchArgs();
        SingleSearchArgs singleSearchArgs = new SingleSearchArgs();

        JCommander cmdline = JCommander.newBuilder()
                .addObject(mainArgs)
                .addCommand("batch", batchSearchArgs)
                .addCommand("singleSearch", singleSearchArgs)
                .build();

        try {
            cmdline.parse(argv);
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder(e.getMessage()).append('\n');
            cmdline.usage(sb);
            JCommander.getConsole().println(sb.toString());
            System.exit(1);
        }

        if (mainArgs.displayHelpMessage || batchSearchArgs.displayHelpMessage || singleSearchArgs.displayHelpMessage) {
            cmdline.usage();
            System.exit(0);
        }

        switch (cmdline.getParsedCommand()) {
            case "batch":
                createOutputDir(batchSearchArgs);
                runBatchSearch(batchSearchArgs);
                break;
            case "singleSearch":
                createOutputDir(singleSearchArgs);
                runSingleSearch(singleSearchArgs);
                break;
        }
    }

    private static void createOutputDir(AbstractArgs args) {
            if (StringUtils.isNotBlank(args.outputDir)) {
                try {
                // create output directory
                Files.createDirectories(Paths.get(args.outputDir));
                } catch (IOException e) {
                    LOG.error("Error creating output directory: {}", args.outputDir, e);
                    System.exit(1);
                }
            }
    }

    private static void runBatchSearch(BatchSearchArgs args) {
        ColorMIPSearch colorMIPSearch;
        if (args.useLocalProcessing) {
            colorMIPSearch = new LocalColorMIPSearch(args.gradientDir, args.outputDir, args.dataThreshold, args.maskThreshold, args.pixColorFluctuation, args.xyShift, args.mirrorMask, args.pctPositivePixels, args.cdsConcurrency, args.hemiMask);
        } else {
            colorMIPSearch = new SparkColorMIPSearch(
                    args.appName, args.gradientDir, args.outputDir, args.dataThreshold, args.maskThreshold, args.pixColorFluctuation, args.xyShift, args.mirrorMask, args.pctPositivePixels, args.hemiMask
            );
        }

        try {
            List<MIPInfo> libraryMips = args.librariesInputs.stream()
                    .flatMap(libraryInput -> readMIPs(libraryInput).stream())
                    .collect(Collectors.toList());

            List<MIPInfo> masksMips = args.masksInputs.stream()
                    .flatMap(masksInput -> readMIPs(masksInput).stream())
                    .collect(Collectors.toList());

            colorMIPSearch.compareEveryMaskWithEveryLibrary(masksMips, libraryMips);
        } finally {
            colorMIPSearch.terminate();
        }
    }

    private static void runSingleSearch(SingleSearchArgs args) {
        LocalColorMIPSearch colorMIPSearch = new LocalColorMIPSearch(args.gradientDir, args.outputDir, args.dataThreshold, args.maskThreshold, args.pixColorFluctuation, args.xyShift, args.mirrorMask, args.pctPositivePixels, args.cdsConcurrency, args.hemiMask);
        try {
            MIPInfo libraryMIP = new MIPInfo();
            libraryMIP.filepath = args.libraryPath;

            MIPInfo maskMIP = new MIPInfo();
            maskMIP.filepath = args.maskPath;

            ColorMIPSearchResult searchResult = colorMIPSearch.runImageComparison(colorMIPSearch.loadMIP(libraryMIP), colorMIPSearch.loadMIP(maskMIP));
            colorMIPSearch.writeSearchResults(args.resultName, Arrays.asList(searchResult.perLibraryMetadata()));
        } finally {
            colorMIPSearch.terminate();
        }
    }

    private static List<MIPInfo> readMIPs(ListArg mipsArg) {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            LOG.info("Reading {}", mipsArg);
            List<MIPInfo> content = mapper.readValue(new File(mipsArg.input), new TypeReference<List<MIPInfo>>() {
            });
            int from = mipsArg.offset > 0 ? mipsArg.offset : 0;
            int to = mipsArg.length > 0 ? Math.min(from + mipsArg.length, content.size()) : content.size();
            LOG.info("Read {} mips from {} starting at {} to {}", content.size(), mipsArg, from, to);
            return content.subList(from, to);
        } catch (IOException e) {
            LOG.error("Error reading {}", mipsArg, e);
            throw new UncheckedIOException(e);
        }
    }
}
