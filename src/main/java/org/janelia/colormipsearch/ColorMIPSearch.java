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

import ij.ImagePlus;
import ij.io.Opener;
import ij.process.ImageProcessor;
import org.apache.spark.SparkConf;
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

    ColorMIPSearch(String appName,
                   Integer dataThreshold, Double pixColorFluctuation, Integer xyShift,
                   boolean mirrorMask, Double pctPositivePixels) {
        this.dataThreshold = dataThreshold;
        this.pixColorFluctuation = pixColorFluctuation;
        this.xyShift = xyShift;
        this.mirrorMask = mirrorMask;
        this.pctPositivePixels = pctPositivePixels;
        this.sparkContext = new JavaSparkContext(new SparkConf().setAppName(appName));
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
        return new ImagePlus(title, ImageIO.read(stream));
    }

    private ImagePlus readTiffToImagePlus(String title, InputStream stream) throws Exception {
        return new Opener().openTiff(stream, title);
    }

    Collection<ColorMIPSearchResult> searchEveryMaskInAllImages(List<String> maskPathnames, Integer maskThreshold) {
        LOG.info("Searching {} masks", maskPathnames.size());
        JavaRDD<String> maskPartitions = sparkContext.parallelize(maskPathnames);
        LOG.info("Mask partitions/count: {} / {}", maskPartitions.getNumPartitions(), maskPartitions.count());

        JavaPairRDD<Tuple2<String, ImagePlus>, String> imageAndMaskPairs = imagePlusRDD.cartesian(maskPartitions);

        JavaRDD<ColorMIPSearchResult> searchResults = imageAndMaskPairs.map(imageAndMaskPair -> {
            String maskPathname = imageAndMaskPair._2;
            LOG.info("Search mask {} against {}", maskPathname, imageAndMaskPair._1._1);
            Path maskPath = Paths.get(maskPathname);
            ImagePlus maskImage = readImagePlus(maskPath.getFileName().toString(), maskPathname);
            return runImageComparison(imageAndMaskPair._1._1, imageAndMaskPair._1._2.getProcessor(), maskPathname, maskImage.getProcessor(), maskThreshold);
        });

        JavaRDD<ColorMIPSearchResult> sortedSearchResultsRDD = searchResults.sortBy(ColorMIPSearchResult::getMatchingSlices, false, 1);
        LOG.info("Sorted search results partitions: {}", sortedSearchResultsRDD.getNumPartitions());

        List<ColorMIPSearchResult> sortedSearchResults = sortedSearchResultsRDD.collect();
        LOG.info("Returned {} results for {} masks", sortedSearchResults, maskPathnames.size());

        return sortedSearchResults;
    }

    List<ColorMIPSearchResult> searchMaskFromFileInAllImages(String maskPathname, Integer maskThreshold) {
        LOG.info("Searching mask {} against {} loaded libraries", maskPathname, imagePlusRDD.count());

        JavaRDD<ColorMIPSearchResult> searchResults = imagePlusRDD.map(imagePathPair -> {
            // Cache mask object at the task level
            Path maskPath = Paths.get(maskPathname);
            ImagePlus maskImage = readImagePlus(maskPath.getFileName().toString(), maskPathname);
            return runImageComparison(imagePathPair._1, imagePathPair._2.getProcessor(), maskPathname, maskImage.getProcessor(), maskThreshold);
        });
        LOG.info("Search results partitions: {}", searchResults.getNumPartitions());

        JavaRDD<ColorMIPSearchResult> sortedSearchResultsRDD = searchResults.sortBy(ColorMIPSearchResult::getMatchingSlices, false, 1);

        LOG.info("Sorted search results partitions: {}", sortedSearchResultsRDD.getNumPartitions());

        List<ColorMIPSearchResult> sortedSearchResults = sortedSearchResultsRDD.collect();

        LOG.info("Returned {} results for mask {}", sortedSearchResults, maskPathname);

        return sortedSearchResults;
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

    void terminate() {
        sparkContext.close();
    }

}
