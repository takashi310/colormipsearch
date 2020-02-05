package org.janelia.colormipsearch;

import java.util.List;
import java.util.stream.Collectors;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
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
        private String appName = ColorMIPSearch.class.getName();

        @Parameter(names = {"--images", "-i"}, description = "Comma-delimited list of directories containing images to search", required = true, variableArity = true)
        private List<String> imageFiles;

        @Parameter(names = {"--masks", "-m"}, description = "Image file(s) to use as the search masks", required = true, variableArity = true)
        private List<String> maskFiles;

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

        @Parameter(names = {"--masksFilesPartitionSize"}, description = "Specify how to partition masks files")
        private Integer masksFilesPartitionSize = 100;

        @Parameter(names = {"--outputDir", "-od"}, description = "Output directory")
        private String outputDir;
    }

    public static void main(String[] argv) {
        Args args = new Args();
        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(argv);

        ColorMIPSearch colorMIPSearch = new ColorMIPSearch(
                args.appName, args.dataThreshold, args.pixColorFluctuation, args.xyShift, args.mirrorMask, args.pctPositivePixels
        );
        try {
            List<String> librariesFiles = ImageFinder.findImages(args.imageFiles).collect(Collectors.toList());
            LOG.info("Found {} libraries files", librariesFiles.size());
            List<String> masksFiles = ImageFinder.findImages(args.maskFiles).collect(Collectors.toList());
            LOG.info("Found {} masks files", masksFiles.size());
            if (librariesFiles.isEmpty() || masksFiles.isEmpty()) {
                LOG.info("Nothing to do - either the libraries or the masks list is empty");
            } else {
                colorMIPSearch.loadImages(librariesFiles);
                colorMIPSearch.compareEveryMaskWithEveryLoadedLibrary(
                        masksFiles,
                        args.maskThreshold,
                        args.masksFilesPartitionSize);
            }
        } finally {
            colorMIPSearch.terminate();
        }
    }

}
