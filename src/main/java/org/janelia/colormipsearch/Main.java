package org.janelia.colormipsearch;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static class Args {

        @Parameter(names = "--app")
        private String appName = "ColorMIPSearch";

        @Parameter(names = {"--images", "-i"}, required = true, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "Comma-delimited list of directories containing images to search")
        private List<ListArg> librariesInputs;

        @Parameter(names = {"--masks", "-m"}, required = true, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "Image file(s) to use as the search masks")
        private List<ListArg> masksInputs;

        @Parameter(names = {"--dataThreshold"}, description = "Data threshold")
        private Integer dataThreshold = 100;

        @Parameter(names = {"--maskThreshold"}, description = "Mask threshold")
        private Integer maskThreshold = 100;

        @Parameter(names = {"--pixColorFluctuation"}, description = "Pix Color Fluctuation, 1.18 per slice")
        private Double pixColorFluctuation = 2.0;

        @Parameter(names = {"--xyShift"}, description = "Number of pixels to try shifting in XY plane")
        private Integer xyShift = 0;

        @Parameter(names = {"--mirrorMask"}, description = "Should the mask be mirrored across the Y axis?")
        private boolean mirrorMask = false;

        @Parameter(names = {"--pctPositivePixels"}, description = "% of Positive PX Threshold (0-100%)")
        private Double pctPositivePixels = 0.0;

        @Parameter(names = {"--outputDir", "-od"}, description = "Output directory")
        private String outputDir;

        @Parameter(names = "-locally", description = "Perform the search in the current process", arity = 0)
        private boolean useLocalProcessing = false;

        @Parameter(names = "-cdsConcurrency", description = "CDS concurrency - number of CDS tasks run concurrently")
        private int cdsConcurrency = 100;

        @Parameter(names = "-h", description = "Display the help message", help = true, arity = 0)
        private boolean displayHelpMessage = false;
    }

    public static void main(String[] argv) {
        Args args = new Args();
        JCommander cmdline = JCommander.newBuilder()
                .addObject(args)
                .build();

        try {
            cmdline.parse(argv);
        } catch (Exception e) {
            cmdline.usage();
            System.exit(1);
        }

        if (args.displayHelpMessage) {
            cmdline.usage();
            System.exit(0);
        }

        try {
            // create output directory
            Files.createDirectories(Paths.get(args.outputDir));
        } catch (IOException e) {
            LOG.error("Error creating output directory", e);
            System.exit(1);
        }

        ColorMIPSearch colorMIPSearch;
        if (args.useLocalProcessing) {
            colorMIPSearch = new LocalColorMIPSearch(args.outputDir, args.dataThreshold, args.pixColorFluctuation, args.xyShift, args.mirrorMask, args.pctPositivePixels, args.cdsConcurrency);
        } else {
            colorMIPSearch = new SparkColorMIPSearch(
                    args.appName, args.outputDir, args.dataThreshold, args.pixColorFluctuation, args.xyShift, args.mirrorMask, args.pctPositivePixels
            );
        }

        try {
            ObjectMapper mapper = new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            ;

            List<MIPInfo> libraryMips = args.librariesInputs.stream()
                    .flatMap(libraryInput -> readMIPs(libraryInput).stream())
                    .collect(Collectors.toList());

            List<MIPInfo> masksMips = args.masksInputs.stream()
                    .flatMap(masksInput -> readMIPs(masksInput).stream())
                    .collect(Collectors.toList());

            colorMIPSearch.compareEveryMaskWithEveryLibrary(masksMips, libraryMips, args.maskThreshold);
        } finally {
            colorMIPSearch.terminate();
        }
    }

    private static List<MIPInfo> readMIPs(ListArg mipsArg) {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            LOG.info("Reading {}", mipsArg);
            List<MIPInfo> content = mapper.readValue(new File(mipsArg.input), new TypeReference<List<MIPInfo>>() {});
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
