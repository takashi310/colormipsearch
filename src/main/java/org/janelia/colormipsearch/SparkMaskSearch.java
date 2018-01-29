package org.janelia.colormipsearch;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import ij.ImagePlus;
import ij.io.Opener;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.input.PortableDataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import javax.imageio.ImageIO;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SparkMaskSearch implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(SparkMaskSearch.class);

    private transient final JavaSparkContext context;
    private transient JavaPairRDD<String, ImagePlus> imagePlusRDD;
    private Integer dataThreshold;
    private Double pixColorFluctuation;
    private Double pctPositivePixels;

    public SparkMaskSearch(Integer dataThreshold, Double pixColorFluctuation, Double pctPositivePixels) {
        this.dataThreshold = dataThreshold;
        this.pixColorFluctuation = pixColorFluctuation;
        this.pctPositivePixels = pctPositivePixels;
        SparkConf conf = new SparkConf().setAppName(SparkMaskSearch.class.getName());
        this.context = new JavaSparkContext(conf);
    }

    private ImagePlus readPngToImagePlus(String title, InputStream stream) throws Exception {
        return new ImagePlus(title, ImageIO.read(stream));
    }

    private ImagePlus readTiffToImagePlus(String title, InputStream stream) throws Exception {
        return new Opener().openTiff(stream, title);
//        mask = new Opener().deserialize(bytes);
    }

    private ImagePlus readImagePlus(String filepath, String title, InputStream stream) throws Exception {
        switch (getImageFormat(filepath)) {
            case PNG:
                return readPngToImagePlus(title, stream);
            case TIFF:
                return readTiffToImagePlus(title, stream);
        }
        throw new IllegalArgumentException("Mask must be in PNG or TIFF format");
    }

    private ImagePlus readImagePlus(String filepath, String title, PortableDataStream stream) throws Exception {
        try (DataInputStream dis = stream.open()) {
            return readImagePlus(filepath, title, dis);
        }
    }

    private MaskSearchResult search(String filepath, ImagePlus image, ImagePlus mask, Integer maskThreshold) throws Exception {

        if (image==null) {
            log.error("Problem loading image: {}", filepath);
            return new MaskSearchResult(filepath, 0, 0, false);
        }

        log.info("Searching "+filepath);

        ColorMIPMaskCompare.Parameters params = new ColorMIPMaskCompare.Parameters();
        params.maskImage = mask;
        params.searchImage = image;
        params.pixflu = pixColorFluctuation;
        params.pixThres = pctPositivePixels;
        params.Thres = dataThreshold;
        params.Thresm = maskThreshold;
        ColorMIPMaskCompare search = new ColorMIPMaskCompare();
        ColorMIPMaskCompare.Output output = search.runSearch(params);

        return new MaskSearchResult(filepath, output.matchingSlices, output.matchingSlicesPct, output.isMatch);
    }

    /**
     * Load an image archive into memory.
     * @param imagesFilepath
     */
    public void loadImages(String imagesFilepath) {

        // We have to ensure each filepath has a glob, because it's very slow without.
        // See https://issues.apache.org/jira/browse/SPARK-8437
        StringBuffer filepaths = new StringBuffer();
        for(String filepath : imagesFilepath.split(",")) {
            if (!filepath.contains("*")) {
                if (!filepath.endsWith("/")) {
                    filepath += "/";
                }
                filepath += "*";
            }
            if (filepaths.length()>0) filepaths.append(",");
            filepaths.append(filepath);
        }

        log.info("Loading image archive at: {}", filepaths);
        log.info("Default parallelism: {}", context.defaultParallelism());

        JavaPairRDD<String, PortableDataStream> filesRdd = context.binaryFiles(filepaths.toString());
        log.info("filesRdd.numPartitions: {}", filesRdd.getNumPartitions());

        this.imagePlusRDD = filesRdd.mapToPair(pair -> new Tuple2<>(pair._1, readImagePlus(pair._1, "search", pair._2))).cache();
        log.info("imagePlusRDD.numPartitions: {}", imagePlusRDD.getNumPartitions());
        log.info("imagePlusRDD.count: {}", imagePlusRDD.count());
    }

    /**
     * Perform the search.
     * @param maskFilepath
     * @return
     * @throws Exception
     */
    public Collection<MaskSearchResult> search(String maskFilepath, Integer maskThreshold) throws Exception {

        log.info("Searching with mask: {}", maskFilepath);

        // Read mask file
        byte[] maskBytes = Files.readAllBytes(Paths.get(maskFilepath));
        log.info("Loaded {} bytes for mask file", maskBytes.length);

        // Send mask to all workers
        Broadcast<byte[]> maskHandle = context.broadcast(maskBytes);
        log.info("Broadcast mask file as {}", maskHandle.id());

        JavaRDD<MaskSearchResult> resultRdd = imagePlusRDD.map(pair -> {
            log.info("Deserializing mask bytes..");
            byte[] bytes = maskHandle.value();
            log.info("Got {} mask bytes", bytes.length);

            ImagePlus mask;
            try (ByteArrayInputStream stream = new ByteArrayInputStream(bytes)) {
                mask = readImagePlus(maskFilepath, "mask", stream);
            }

            return search(pair._1, pair._2, mask, maskThreshold);
        });
        log.info("resultRdd.numPartitions: {}", resultRdd.getNumPartitions());

        JavaRDD<MaskSearchResult> sortedResultRdd = resultRdd.sortBy(result -> result.getMatchingSlices(), false, 1);
        log.info("sortedResultRdd.numPartitions: {}", sortedResultRdd.getNumPartitions());

        List<MaskSearchResult> results = sortedResultRdd.collect();
        log.info("Returning {} results", results.size());
        return results;
    }

    private enum ImageFormat {
        PNG,
        TIFF,
        UNKNOWN
    }

    private ImageFormat getImageFormat(String filepath) {

        String lowerPath = filepath.toLowerCase();

        if (lowerPath.endsWith(".png")) {
            return ImageFormat.PNG;
        }
        else if (lowerPath.endsWith(".tiff") || lowerPath.endsWith(".tif")) {
            return ImageFormat.TIFF;
        }

        return ImageFormat.UNKNOWN;
    }

    public void close() {
        if (context!=null) context.stop();
    }

    public static class Args {

        @Parameter(names = {"--mask", "-m"}, description = "Image file(s) to use as the search masks", required = true, variableArity = true)
        private List<String> maskFiles;

        @Parameter(names = {"--imageDir", "-i"}, description = "Comma-delimited list of directories containing images to search", required = true)
        private String imageDir;

        @Parameter(names = {"--dataThreshold"}, description = "Data threshold")
        private Integer dataThreshold = 100;

        @Parameter(names = {"--maskThresholds"}, description = "Mask thresholds", variableArity = true)
        private List<Integer> maskThresholds;

        @Parameter(names = {"--pixColorFluctuation"}, description = "Pix Color Fluctuation, 1.18 per slice")
        private Double pixColorFluctuation = 2.0;

        @Parameter(names = {"--pctPositivePixels"}, description = "% of Positive PX Threshold (0-100%)")
        private Double pctPositivePixels = 2.0;

        @Parameter(names = {"--outputFile", "-o"}, description = "Output file(s) for results in CSV format. " +
                "If this is not specified, the output will be printed to the log. " +
                "If this is specified, then there should be one output file per mask file.", variableArity = true)
        private List<String> outputFiles;
    }

    public static void main(String[] argv) throws Exception {

        Args args = new Args();
        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(argv);

        Integer dataThreshold = args.dataThreshold;
        Double pixColorFluctuation = args.pixColorFluctuation;
        Double pctPositivePixels = args.pctPositivePixels;

        if (args.maskThresholds != null) {
            if (args.maskThresholds.size() != args.maskFiles.size()) {
                throw new ParameterException("Number of mask thresholds must match the number of masks used");
            }
        }

        if (args.outputFiles != null) {
            if (args.maskFiles.size() != args.outputFiles.size()) {
                throw new ParameterException("Number of output files must match the number of masks used");
            }
        }

        SparkMaskSearch sparkMaskSearch = new SparkMaskSearch(dataThreshold, pixColorFluctuation, pctPositivePixels);

        try {
            sparkMaskSearch.loadImages(args.imageDir);
            int i=0;
            for(String maskFile : args.maskFiles) {

                Integer maskThreshold = 50;
                if (args.maskThresholds != null) {
                    maskThreshold = args.maskThresholds.get(i);
                }

                Stream<MaskSearchResult> results = sparkMaskSearch.search(maskFile, maskThreshold).stream().filter(r -> r.isMatch());

                if (args.outputFiles != null) {
                    String outputFile = args.outputFiles.get(i);
                    log.info("Writing search results for {} to {}", maskFile, outputFile);
                    try (PrintWriter printWriter = new PrintWriter(outputFile)) {
                        printWriter.println(maskFile);
                        results.forEach(r -> {
                            String filepath = r.getFilepath().replaceFirst("^file:", "");
                            printWriter.printf("%d\t%#.5f\t%s\n", r.getMatchingSlices(), r.getMatchingSlicesPct(), filepath);
                        });
                    }
                } else {
                    log.info("Search results for {}:", maskFile);
                    results.forEach(r -> {
                        log.info("{} - {}", r.getMatchingSlicesPct(), r.getFilepath());
                    });
                }

                i++;
            }
        }
        finally {
            sparkMaskSearch.close();
        }
    }

}
