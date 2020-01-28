package org.janelia.colormipsearch;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.imageio.ImageIO;

import com.google.common.base.Stopwatch;

import ij.ImagePlus;
import ij.io.Opener;
import ij.process.ImageProcessor;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
class ColorMIPSearch implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(ColorMIPSearch.class);

    private enum ImageFormat {
        PNG,
        TIFF,
        UNKNOWN
    }

    private static final int ERROR_THRESHOLD = 20;

    private Integer dataThreshold;
    private Integer xyShift;
    private boolean mirrorMask;
    private Double pixColorFluctuation;
    private Double pctPositivePixels;
    private transient final JavaSparkContext sparkContext;
    private transient JavaPairRDD<String, ImagePlus> imagePlusRDD;
    private transient ImagePlus maskImage;


    ColorMIPSearch(Integer dataThreshold, Double pixColorFluctuation, Integer xyShift,
                   boolean mirrorMask, Double pctPositivePixels,
                   JavaSparkContext sparkContext) {
        this.dataThreshold = dataThreshold;
        this.pixColorFluctuation = pixColorFluctuation;
        this.xyShift = xyShift;
        this.mirrorMask = mirrorMask;
        this.pctPositivePixels = pctPositivePixels;
        this.sparkContext = sparkContext;
    }

    /**
     * Load provided image libraries into memory.
     * @param imagesFiles
     */
    void loadImages(List<String> imagesFiles) {
        List<String> imagePaths = ImageFinder.findImages(imagesFiles)
                .collect(Collectors.collectingAndThen(
                        Collectors.toList(),
                        list -> {
                            // Randomize path list so that each task has some paths from each directory. Otherwise, some tasks would only get
                            // files from an "easy" directory where all the files are small
                            Collections.shuffle(list);
                            return list;
                        }
                ))
                ;

        LOG.info("Total paths: {}", imagePaths.size());
        LOG.info("Default parallelism: {}", sparkContext.defaultParallelism());

        // This is a lot faster than using binaryFiles because 1) the paths are shuffled, 2) we use an optimized
        // directory listing stream which does not consider file sizes. As a bonus, it actually respects the parallelism
        // setting, unlike binaryFiles which ignores it unless you set other arcane settings like openCostInByte.
        JavaRDD<String> imagePathsRDD = sparkContext.parallelize(imagePaths);
        LOG.info("filesRdd.numPartitions: {}", imagePathsRDD.getNumPartitions());

        // This RDD is cached so that it can be reused to search with multiple masks
        this.imagePlusRDD = imagePathsRDD.mapToPair(filepath -> {
            String title = new File(filepath).getName();
            return new Tuple2<>(filepath, readImagePlus(title, filepath));
        }).cache();

        LOG.info("imagePlusRDD.numPartitions: {}", imagePlusRDD.getNumPartitions());
        LOG.info("imagePlusRDD.count: {}", imagePlusRDD.count());
    }

    private ImagePlus readImagePlus(String title, String filepath) throws Exception {
        return readImagePlus(title, getImageFormat(filepath), new FileInputStream(filepath));
    }

    private ImagePlus readImagePlus(String title, ImageFormat format, InputStream stream) throws Exception {
        switch (format) {
            case PNG:
                return readPngToImagePlus(title, stream);
            case TIFF:
                return readTiffToImagePlus(title, stream);
        }
        throw new IllegalArgumentException("Image must be in PNG or TIFF format");

    }

    private ImageFormat getImageFormat(String filepath) {
        String lowerPath = filepath.toLowerCase();

        if (lowerPath.endsWith(".png")) {
            return ImageFormat.PNG;
        } else if (lowerPath.endsWith(".tiff") || lowerPath.endsWith(".tif")) {
            return ImageFormat.TIFF;
        }

        LOG.info("Image format unknown: {}", filepath);
        return ImageFormat.UNKNOWN;
    }

    private ImagePlus readPngToImagePlus(String title, InputStream stream) throws Exception {
        Stopwatch s = Stopwatch.createStarted();
        ImagePlus imagePlus = new ImagePlus(title, ImageIO.read(stream));
        LOG.info("Reading {} took {} ms", title, s.elapsed().toMillis());
        return imagePlus;
    }

    private ImagePlus readTiffToImagePlus(String title, InputStream stream) throws Exception {
        Stopwatch s = Stopwatch.createStarted();
        ImagePlus imagePlus = new Opener().openTiff(stream, title);
        LOG.info("Reading {} took {} ms", title, s.elapsed().toMillis());
        return imagePlus;
    }

    Collection<ColorMIPSearchResult> searchMasksInAllImages(List<String> maskPathnames, Integer maskThreshold) {
        Stopwatch s = Stopwatch.createStarted();

        LOG.info("Searching {} masks", maskPathnames.size());
        JavaRDD<String> maskPartitions = sparkContext.parallelize(maskPathnames);
        JavaRDD<ColorMIPSearchResult> searchResults = maskPartitions.flatMap(maskPathname -> searchSingleMaskInAllImages(maskPathname, maskThreshold).iterator());

        LOG.info("Search results partitions for {} masks: {}", maskPathnames.size(), searchResults.getNumPartitions());

        List<ColorMIPSearchResult> results = searchResults.collect();
        LOG.info("Returning {} results", results.size());

        LOG.info("Searching took {} ms", s.elapsed().toMillis());
        return results;
    }

    private Collection<ColorMIPSearchResult> searchSingleMaskInAllImages(String maskPathname, Integer maskThreshold) {
        Stopwatch s = Stopwatch.createStarted();

        LOG.info("Searching mask {}", maskPathname);
        Path maskPath = Paths.get(maskPathname);
        byte[] maskBytes;
        try {
            // Read mask bytes in the driver
            maskBytes = Files.readAllBytes(maskPath);
            LOG.info("Loaded {} bytes from mask file {}", maskBytes.length, maskPath);
        } catch (IOException e) {
            LOG.warn("Error reading {} bytes", maskPath, e);
            throw new UncheckedIOException(e);
        }
        // Send mask bytes to all workers
        Broadcast<byte[]> maskHandle = sparkContext.broadcast(maskBytes);
        LOG.info("Broadcast mask file {} as {}", maskPath, maskHandle.id());

        JavaRDD<ColorMIPSearchResult> searchResults = imagePlusRDD.map(pair -> {
            // Cache mask object at the task level
            if (maskImage == null) {
                byte[] bytes = maskHandle.value();
                LOG.info("Got {} mask bytes", bytes.length);
                try (ByteArrayInputStream stream = new ByteArrayInputStream(bytes)) {
                    maskImage = readImagePlus(maskPath.getFileName().toString(), getImageFormat(maskPathname), stream);
                }
            }
            return runImageComparison(pair._1, pair._2.getProcessor(), maskPathname, maskImage.getProcessor(), maskThreshold);
        });
        LOG.info("Search results partitions: {}", searchResults.getNumPartitions());

        JavaRDD<ColorMIPSearchResult> sortedSearchResults = searchResults.sortBy(ColorMIPSearchResult::getMatchingSlices, false, 1);

        LOG.info("Sorted search results partitions: {}", sortedSearchResults.getNumPartitions());

        List<ColorMIPSearchResult> results = sortedSearchResults.collect();
        LOG.info("Returning {} results", results.size());

        LOG.info("Searching took {} ms", s.elapsed().toMillis());
        return results;
    }

    private ColorMIPSearchResult runImageComparison(String libraryImagePath, ImageProcessor libraryImage,
                                                    String patternImagePath, ImageProcessor patternImage,
                                                    Integer searchThreshold) {
        try {
            LOG.info("Compare library file {} with mask {}", libraryImagePath,  patternImagePath);

            double pixfludub = pixColorFluctuation / 100;

            final ColorMIPMaskCompare cc = new ColorMIPMaskCompare(
                    patternImage, searchThreshold, mirrorMask, null, 0,
                    mirrorMask, dataThreshold, pixfludub, xyShift);
            ColorMIPMaskCompare.Output output = cc.runSearch(libraryImage, null);

            double pixThresdub = pctPositivePixels / 100;
            boolean isMatch = output.matchingPct > pixThresdub;

            return new ColorMIPSearchResult(patternImagePath, libraryImagePath, output.matchingPixNum, output.matchingPct, isMatch, false);
        } catch (Throwable e) {
            LOG.info("Error comparing library file {} with mask {}", libraryImagePath,  patternImagePath, e);
            return new ColorMIPSearchResult(patternImagePath, libraryImagePath, 0, 0, false, true);
        }
    }
}
