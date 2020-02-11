package org.janelia.colormipsearch;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.SubParameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;

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

    private static class MIPListArg {
        @SubParameter(order = 0)
        String inputFilename;
        @SubParameter(order = 1)
        int offset = 0;
        @SubParameter(order = 2)
        int length = -1;

        private void setOffset(int offset) {
            if (offset > 0) {
                this.offset = offset;
            } else {
                this.offset = 0;
            }
        }

        private void setLength(int length) {
            this.length = length > 0 ? length : -1;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(inputFilename);
            if (offset > 0 || length > 0) {
                sb.append(':');
            }
            if (offset > 0) {
                sb.append(offset);
            }
            if (length > 0) {
                sb.append(':').append(length);
            }
            return sb.toString();
        }
    }

    private static class MIPListArgConverter implements IStringConverter<MIPListArg> {
        @Override
        public MIPListArg convert(String value) {
            List<String> argComponents = Splitter.on(":").trimResults().splitToList(value);
            MIPListArg arg = new MIPListArg();
            if (argComponents.size() > 0) {
                arg.inputFilename = argComponents.get(0);
            }
            if (argComponents.size() > 1 && StringUtils.isNotBlank(argComponents.get(1))) {
                arg.setOffset(Integer.parseInt(argComponents.get(1)));
            }
            if (argComponents.size() > 2 && StringUtils.isNotBlank(argComponents.get(2))) {
                arg.setLength(Integer.parseInt(argComponents.get(2)));
            }
            return arg;
        }
    }

    private static class Args {

        @Parameter(names = "--app")
        private String appName = "ColorMIPSearch";

        @Parameter(names = {"--images", "-i"}, required = true, variableArity = true, converter = MIPListArgConverter.class,
                description = "Comma-delimited list of directories containing images to search")
        private List<MIPListArg> librariesInputs;

        @Parameter(names = {"--masks", "-m"}, required = true, variableArity = true, converter = MIPListArgConverter.class,
                description = "Image file(s) to use as the search masks")
        private List<MIPListArg> masksInputs;

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
                    .flatMap(libraryInput -> readMIPs(libraryInput).stream())
                    .collect(Collectors.toList());

            List<MinimalColorDepthMIP> masksMips = args.masksInputs.stream()
                    .flatMap(masksInput -> readMIPs(masksInput).stream())
                    .collect(Collectors.toList());

            colorMIPSearch.compareEveryMaskWithEveryLibrary(masksMips, libraryMips, args.maskThreshold);
        } finally {
            colorMIPSearch.terminate();
        }
    }

    private static List<MinimalColorDepthMIP> readMIPs(MIPListArg mipsArg) {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            LOG.info("Reading {}", mipsArg);
            List<MinimalColorDepthMIP> content = mapper.readValue(new File(mipsArg.inputFilename), new TypeReference<List<MinimalColorDepthMIP>>() {});
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
