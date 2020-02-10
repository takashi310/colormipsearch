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

        @Parameter(names = {"--images", "-i"}, description = "Comma-delimited list of directories containing images to search", required = true, variableArity = true)
        private List<String> librariesInputs;

        @Parameter(names = {"--masks", "-m"}, description = "Image file(s) to use as the search masks", required = true, variableArity = true)
        private List<String> masksInputs;

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
            colorMIPSearch = new LocalColorMIPSearch(args.outputDir, args.dataThreshold, args.pixColorFluctuation, args.xyShift, args.mirrorMask, args.pctPositivePixels);
        } else {
            colorMIPSearch = new SparkColorMIPSearch(
                    args.appName, args.outputDir, args.dataThreshold, args.pixColorFluctuation, args.xyShift, args.mirrorMask, args.pctPositivePixels
            );
        }

        try {
            ObjectMapper mapper = new ObjectMapper()
                                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            ;

            List<MinimalColorDepthMIP> libraryMips = args.librariesInputs.stream()
                    .flatMap(libraryInput -> {
                        try {
                            List<MinimalColorDepthMIP> content = mapper.readValue(new File(libraryInput), new TypeReference<List<MinimalColorDepthMIP>>(){});
                            LOG.info("Read {} mips from library {}", content.size(), libraryInput);
                            return content.stream();
                        } catch (IOException e) {
                            LOG.error("Error reading {}", libraryInput, e);
                            throw new UncheckedIOException(e);
                        }
                    })
                    .collect(Collectors.toList());

            List<MinimalColorDepthMIP> masksMips = args.masksInputs.stream()
                    .flatMap(masksInput -> {
                        try {
                            List<MinimalColorDepthMIP> content = mapper.readValue(new File(masksInput), new TypeReference<List<MinimalColorDepthMIP>>(){});
                            LOG.info("Read {} mips from mask {}", content.size(), masksInput);
                            return content.stream();
                        } catch (IOException e) {
                            LOG.error("Error reading {}", masksInput, e);
                            throw new UncheckedIOException(e);
                        }
                    })
                    .collect(Collectors.toList());

            colorMIPSearch.compareEveryMaskWithEveryLibrary(masksMips, libraryMips, args.maskThreshold);
        } finally {
            colorMIPSearch.terminate();
        }
    }

}
