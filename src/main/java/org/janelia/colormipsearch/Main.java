package org.janelia.colormipsearch;

import java.util.List;
import java.util.stream.Collectors;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class Main {

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
        private Integer maskThreshold;

        @Parameter(names = {"--pixColorFluctuation"}, description = "Pix Color Fluctuation, 1.18 per slice")
        private Double pixColorFluctuation = 2.0;

        @Parameter(names = {"--xyShift"}, description = "Number of pixels to try shifting in XY plane")
        private Integer xyShift = 0;

        @Parameter(names = {"--mirrorMask"}, description = "Should the mask be mirrored across the Y axis?")
        private boolean mirrorMask = false;

        @Parameter(names = {"--pctPositivePixels"}, description = "% of Positive PX Threshold (0-100%)")
        private Double pctPositivePixels = 2.0;

        @Parameter(names = {"--outputDir", "-od"}, description = "Output directory")
        private String outputDir;
    }

    public static void main(String[] argv) throws Exception {

        Args args = new Args();
        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(argv);

        SparkConf sparkConf = new SparkConf().setAppName(args.appName);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        try {
            ColorMIPSearch colorMIPSearch = new ColorMIPSearch(
                    args.dataThreshold, args.pixColorFluctuation, args.xyShift, args.mirrorMask, args.pctPositivePixels, sparkContext
            );
            colorMIPSearch.loadImages(args.imageFiles);
            colorMIPSearch.searchMasksInAllImages(ImageFinder.findImages(args.maskFiles).collect(Collectors.toList()), args.maskThreshold);
        } finally {
            sparkContext.close();
        }
    }

}
