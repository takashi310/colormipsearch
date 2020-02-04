package org.janelia.colormipsearch;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.imageio.ImageIO;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import ij.ImagePlus;
import ij.io.Opener;
import ij.process.ImageProcessor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

    private enum ResultGroupingCriteria {
        BY_LIBRARY,
        BY_MASK;
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
     * @param imagePaths
     */
    void loadImages(List<String> imagePaths) {
        // Randomize path list so that each task has some paths from each directory. Otherwise, some tasks would only get
        // files from an "easy" directory where all the files are small
        Collections.shuffle(imagePaths);

        LOG.info("Total paths: {}", imagePaths.size());
        LOG.info("Default parallelism: {}", sparkContext.defaultParallelism());

        // This is a lot faster than using binaryFiles because 1) the paths are shuffled, 2) we use an optimized
        // directory listing stream which does not consider file sizes. As a bonus, it actually respects the parallelism
        // setting, unlike binaryFiles which ignores it unless you set other arcane settings like openCostInByte.
        JavaRDD<String> imagePathsRDD = sparkContext.parallelize(imagePaths);
        LOG.info("filesRdd.numPartitions: {}", imagePathsRDD.getNumPartitions());

        // This RDD is cached so that it can be reused to search with multiple masks
        imagePlusRDD = imagePathsRDD.mapToPair(filepath -> {
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

    void  compareEveryMaskWithEveryLoadedLibrary(List<String> masksPaths, Integer maskThreshold, int defaultPartitionSize) {
        LOG.info("Searching {} masks against {} libraries", masksPaths.size(), imagePlusRDD.count());

        BiFunction<Tuple2<List<List<String>>, List<String>>, String, Tuple2<List<List<String>>, List<String>>> partitionAcumulator = (partitionResult, s) -> {
            List<String> currentPartition;
            if (partitionResult._2.size() == defaultPartitionSize) {
                currentPartition = new ArrayList<>();
            } else {
                currentPartition = partitionResult._2;
            }
            currentPartition.add(s);
            if (currentPartition.size() == 1) {
                partitionResult._1.add(currentPartition);
            }
            return new Tuple2<>(partitionResult._1, currentPartition);
        };
        List<List<String>> masksPartitions = masksPaths.stream().reduce(
                new Tuple2<>(new ArrayList<>(), new ArrayList<>()),
                partitionAcumulator,
                (r1, r2) -> r2._1.stream().flatMap(p -> p.stream())
                        .map(s -> partitionAcumulator.apply(r1, s))
                        .reduce((first, second) -> second)
                        .orElse(r1))._1
                ;

        JavaRDD<List<String>> masksRDDPartitions = sparkContext.parallelize(masksPartitions);
        LOG.info("Masks partitions/count: {} / {}", masksRDDPartitions.getNumPartitions(), masksRDDPartitions.count());

        JavaPairRDD<Tuple2<String, ImagePlus>, List<String>> libraryImageAndMasksPairsRDD = imagePlusRDD.cartesian(masksRDDPartitions);
        LOG.info("Library masks cartesian product size: {} / {} (partitions/count)", libraryImageAndMasksPairsRDD.getNumPartitions(), libraryImageAndMasksPairsRDD.count());


        JavaRDD<ColorMIPSearchResult> searchResults = libraryImageAndMasksPairsRDD.mapPartitions(libraryImageAndMasksItr -> {
            Map<String, List<Tuple2<String, Tuple2<String, ImagePlus>>>> maskWithAllImagesFromCurrentPartitionMapping = StreamSupport.stream(Spliterators.spliterator(libraryImageAndMasksItr, Integer.MAX_VALUE, 0), false)
                    .flatMap(libraryImageAndMasks -> libraryImageAndMasks._2.stream().map(maskPath -> new Tuple2<>(maskPath, libraryImageAndMasks._1)))
                    .collect(Collectors.groupingBy(maskAndImagePair -> maskAndImagePair._1, Collectors.toList()));

            return maskWithAllImagesFromCurrentPartitionMapping.entrySet()
                    .stream()
                    .flatMap(maskWithImagesEntry -> {
                        String maskPathname = maskWithImagesEntry.getKey();
                        LOG.debug("Load mask image from {}", maskPathname);
                        Path maskPath = Paths.get(maskPathname);
                        ImagePlus maskImage;
                        try {
                            maskImage = readImagePlus(maskPath.getFileName().toString(), maskPathname);
                        } catch (Exception e) {
                            LOG.error("Error loading {}", maskPathname, e);
                            throw new IllegalStateException("Error loading " + maskPathname, e);
                        }
                        return maskWithImagesEntry.getValue().stream()
                                .map(maskWithImage -> {
                                    Preconditions.checkArgument(maskPathname.equals(maskWithImage._1)); // extra check to ensure the mask is the same as the one we loaded

                                    return new Tuple2<>(maskWithImage._2, new Tuple2<>(maskWithImage._1, maskImage));
                                });
                    })
                    .map((Tuple2<Tuple2<String, ImagePlus>, Tuple2<String, ImagePlus>> imageAndMaskPair) -> runImageComparison(imageAndMaskPair._1._1, imageAndMaskPair._1._2.getProcessor(), imageAndMaskPair._2._1, imageAndMaskPair._2._2.getProcessor(), maskThreshold))
                    .iterator();
        });

        LOG.info("Found {} results in {} partitions", searchResults.count(), searchResults.getNumPartitions());

        // write results for each library
        LOG.info("Write results for each library item");
        writeAllSearchResults(searchResults, ResultGroupingCriteria.BY_LIBRARY);
        // write results for each mask
        LOG.info("Write results for each mask");
        writeAllSearchResults(searchResults, ResultGroupingCriteria.BY_MASK);
    }

    private ColorMIPSearchResult runImageComparison(String libraryImagePath, ImageProcessor libraryImage,
                                                    String patternImagePath, ImageProcessor patternImage,
                                                    Integer searchThreshold) {
        try {
            LOG.info("Compare library file {} with mask {} using threshold {}", libraryImagePath,  patternImagePath, searchThreshold);

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

    private void writeAllSearchResults(JavaRDD<ColorMIPSearchResult> searchResults, ResultGroupingCriteria groupingCriteria) {
        JavaPairRDD<String, Iterable<ColorMIPSearchResult>> groupedSearchResults = searchResults.groupBy(sr -> {
            switch (groupingCriteria) {
                case BY_MASK:
                    return sr.getPatternFilepath();
                case BY_LIBRARY:
                    return sr.getLibraryFilepath();
                default:
                    throw new IllegalArgumentException("Invalid grouping criteria");
            }
        });
        groupedSearchResults.combineByKey(
                srForKey -> sparkContext.parallelize(Lists.newArrayList(srForKey)),
                (srForKeyRDD, srForKey) -> srForKeyRDD.union(sparkContext.parallelize(Lists.newArrayList(srForKey))),
                (sr1RDD, sr2RDD) -> sr1RDD.union(sr2RDD))
                .foreach(keyWithSearchResults -> {
                    LOG.info("Sorting results for {}/{} for {}", keyWithSearchResults._2.getNumPartitions(), keyWithSearchResults._2.count(), keyWithSearchResults._1);
                    JavaRDD<ColorMIPSearchResult> sortedSearchResultsForKey = keyWithSearchResults._2.sortBy(ColorMIPSearchResult::getMatchingSlices, false, 1);
                    sortedSearchResultsForKey.foreach(sr -> {
                        LOG.info("Write search results for {}: {} vs {}", keyWithSearchResults._1, sr.getLibraryFilepath(), sr.getPatternFilepath());
                    });
                });
    }
    
    void terminate() {
        sparkContext.close();
    }

}
