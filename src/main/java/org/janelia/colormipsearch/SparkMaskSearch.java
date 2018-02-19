package org.janelia.colormipsearch;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import ij.ImagePlus;
import ij.io.FileSaver;
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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SparkMaskSearch implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(SparkMaskSearch.class);

    private static final int ERROR_THRESHOLD = 20;

    private transient final JavaSparkContext context;
    private transient JavaPairRDD<String, ImagePlus> imagePlusRDD;
    private Integer dataThreshold;
    private Double pixColorFluctuation;
    private Double pctPositivePixels;
    private transient ImagePlus maskImagePlus;

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
        throw new IllegalArgumentException("Image must be in PNG or TIFF format");
    }

    private ImagePlus readImagePlus(String filepath, String title, PortableDataStream stream) throws Exception {
        try (DataInputStream dis = stream.open()) {
            return readImagePlus(filepath, title, dis);
        }
    }

    private ImagePlus readImagePlus(String filepath, String title) throws Exception {
        switch (getImageFormat(filepath)) {
            case PNG:
                return readPngToImagePlus(title, new FileInputStream(filepath));
            case TIFF:
                return readTiffToImagePlus(title, new FileInputStream(filepath));
        }
        throw new IllegalArgumentException("Image must be in PNG or TIFF format");
    }

    private MaskSearchResult search(String filepath, ImagePlus image, ImagePlus mask, Integer maskThreshold) {

        try {
            if (image == null) {
                log.error("Problem loading image: {}", filepath);
                return new MaskSearchResult(filepath, 0, 0, false, true);
            }

            log.info("Searching " + filepath);

            ColorMIPMaskCompare.Parameters params = new ColorMIPMaskCompare.Parameters();
            params.maskImage = mask;
            params.searchImage = image;
            params.pixflu = pixColorFluctuation;
            params.pixThres = pctPositivePixels;
            params.Thres = dataThreshold;
            params.Thresm = maskThreshold;
            ColorMIPMaskCompare search = new ColorMIPMaskCompare();
            ColorMIPMaskCompare.Output output = search.runSearch(params);

            return new MaskSearchResult(filepath, output.matchingSlices, output.matchingSlicesPct, output.isMatch, false);
        }
        catch (Throwable e) {
            log.error("Problem searching image: {}", filepath, e);
            return new MaskSearchResult(filepath, 0, 0, false, true);
        }
    }

    /**
     * Load an image archive into memory.
     * @param imagesFilepath
     */
    public void loadImages(String imagesFilepath) throws IOException {

        List<String> paths = new ArrayList<>();
        for(String filepath : imagesFilepath.split(",")) {
            log.info("Loading image archive at: {}", filepath);
            Path folder = Paths.get(filepath);
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(folder)) {
                int c = 0;
                for (Path entry : stream) {
                    paths.add(entry.toString());
                    c++;
                }
                log.info("  Read {} files", c);
            }
        }

        // Randomize path list so that each task has some paths from each directory. Otherwise, some tasks would only get
        // files from an "easy" directory where all the files are small
        Collections.shuffle(paths);

        log.info("Total paths: {}", paths.size());
        log.info("Default parallelism: {}", context.defaultParallelism());

        // This is a lot faster than using binaryFiles because 1) the paths are shuffled, 2) we use an optimized
        // directory listing stream which does not consider file sizes. As a bonus, it actually respects the parallelism
        // setting, unlike binaryFiles which ignores it unless you set other arcane settings like openCostInByte.
        JavaRDD<String> pathRDD = context.parallelize(paths);
        log.info("filesRdd.numPartitions: {}", pathRDD.getNumPartitions());

        // This RDD is cached so that it can be reused to search with multiple masks
        this.imagePlusRDD = pathRDD.mapToPair(filepath ->
                new Tuple2<>(filepath, readImagePlus(filepath, "search"))).cache();

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

            // Cache ImagePlus at the task level
            if (maskImagePlus == null) {
                byte[] bytes = maskHandle.value();
                log.info("Got {} mask bytes", bytes.length);
                try (ByteArrayInputStream stream = new ByteArrayInputStream(bytes)) {
                    maskImagePlus = readImagePlus(maskFilepath, "mask", stream);
                }
            }

            return search(pair._1, pair._2, maskImagePlus, maskThreshold);
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

        log.info("Image format unknown: {}", filepath);
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

                Collection<MaskSearchResult> results = sparkMaskSearch.search(maskFile, maskThreshold);

                long numErrors = results.stream().filter(r -> r.isError()).count();

                if (numErrors>ERROR_THRESHOLD) {
                    throw new Exception("Number of search errors exceeded reasonable threshold ("+ERROR_THRESHOLD+")");
                }
                else if (numErrors>0) {
                    log.warn("{} errors encountered while searching. These errors may represent corrupt image files:", numErrors);
                    results.stream().filter(r -> r.isError()).forEach(r -> {
                        log.warn("Error searching {}", r.getFilepath());
                    });
                }

                Stream<MaskSearchResult> matchingResults = results.stream().filter(r -> r.isMatch());

                if (args.outputFiles != null) {
                    String outputFile = args.outputFiles.get(i);
                    log.info("Writing search results for {} to {}", maskFile, outputFile);
                    try (PrintWriter printWriter = new PrintWriter(outputFile)) {
                        printWriter.println(maskFile);
                        matchingResults.forEach(r -> {
                            String filepath = r.getFilepath().replaceFirst("^file:", "");
                            printWriter.printf("%d\t%#.5f\t%s\n", r.getMatchingSlices(), r.getMatchingSlicesPct(), filepath);
                        });
                    }
                } else {
                    log.info("Search results for {}:", maskFile);
                    matchingResults.forEach(r -> {
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
