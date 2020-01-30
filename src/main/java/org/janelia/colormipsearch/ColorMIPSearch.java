package org.janelia.colormipsearch;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.function.BiFunction;
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

    Collection<ColorMIPSearchResult>  searchEveryMaskInEveryLibrary(List<String> masksPaths, List<String> librariesPaths, Integer maskThreshold, int defaultPartitionSize) {
        LOG.info("Searching {} masks against {} libraries", masksPaths.size(), librariesPaths.size());


        JavaPairRDD<String, byte[]> librariesWithContentRDDPartitions = sparkContext.parallelize(librariesPaths).mapToPair(libraryImagePath -> {
            byte[] libraryImageContent = Files.readAllBytes((Paths.get(libraryImagePath)));
            return new Tuple2<>(libraryImagePath, libraryImageContent);
        }).cache();

        LOG.info("Libraries partitions/count: {} / {}", librariesWithContentRDDPartitions.getNumPartitions(), librariesWithContentRDDPartitions.count());
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
                new Tuple2<List<List<String>>, List<String>>(new ArrayList<>(), new ArrayList<>()),
                partitionAcumulator,
                (r1, r2) -> r2._1.stream().flatMap(p -> p.stream())
                        .map(s -> partitionAcumulator.apply(r1, s))
                        .reduce((first, second) -> second)
                        .orElse(r1))._1
                ;

                Lists.partition(masksPaths, defaultPartitionSize);
        JavaRDD<List<String>> masksRDDPartitions = sparkContext.parallelize(masksPartitions);
        LOG.info("Masks partitions/count: {} / {}", masksRDDPartitions.getNumPartitions(), masksRDDPartitions.count());

        JavaPairRDD<Tuple2<String, byte[]>, List<String>> libraryImageAndMasksPairsRDD = librariesWithContentRDDPartitions.cartesian(masksRDDPartitions);
        LOG.info("Library masks cartesian product size: {} / {} (partitions/count)", libraryImageAndMasksPairsRDD.getNumPartitions(), libraryImageAndMasksPairsRDD.count());


        JavaRDD<ColorMIPSearchResult> searchResults = libraryImageAndMasksPairsRDD.mapPartitions(libraryImageAndMasksItr -> {
            Map<String, List<Tuple2<String, Tuple2<String, ImagePlus>>>> maskWithAllImagesFromCurrentPartitionMapping = StreamSupport.stream(Spliterators.spliterator(libraryImageAndMasksItr, Integer.MAX_VALUE, 0), false)
                    .flatMap(libraryImageAndMasks -> libraryImageAndMasks._2.stream().map(maskPath -> new Tuple2<>(maskPath, libraryImageAndMasks._1)))
                    .map(maskAndImageBytesPair -> {
                        String imagePathname = maskAndImageBytesPair._2._1;
                        LOG.debug("Load library image {} from {} bytes", imagePathname, maskAndImageBytesPair._2._2.length);
                        Path imagePath = Paths.get(imagePathname);
                        ImagePlus libraryImage;
                        try {
                            libraryImage = readImagePlus(imagePath.getFileName().toString(), getImageFormat(imagePathname), new ByteArrayInputStream(maskAndImageBytesPair._2._2));
                        } catch (Exception e) {
                            LOG.error("Error loading {}", imagePathname, e);
                            throw new IllegalStateException("Error loading " + imagePathname, e);
                        }
                        return new Tuple2<>(maskAndImageBytesPair._1, new Tuple2<>(imagePathname, libraryImage));
                    })
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

        JavaRDD<ColorMIPSearchResult> sortedSearchResultsRDD = searchResults.sortBy(ColorMIPSearchResult::getMatchingSlices, false, 1);
        LOG.info("Sorted search results partitions: {}", sortedSearchResultsRDD.getNumPartitions());

        List<ColorMIPSearchResult> sortedSearchResults = sortedSearchResultsRDD.collect();
        LOG.info("Returned {} results for {} masks", sortedSearchResults.size(), masksPaths.size());

        return sortedSearchResults;
    }

    Collection<ColorMIPSearchResult> searchEveryMaskInAllImages(List<String> maskPathnames, Integer maskThreshold) {
        LOG.info("Searching {} masks against {} libraries", maskPathnames.size(), imagePlusRDD.count());
        JavaRDD<String> maskPartitions = sparkContext.parallelize(maskPathnames);
        LOG.info("Mask partitions/count: {} / {}", maskPartitions.getNumPartitions(), maskPartitions.count());

        JavaPairRDD<Tuple2<String, ImagePlus>, String> imageAndMaskPairs = imagePlusRDD.cartesian(maskPartitions);
        LOG.info("Image and mask pairs partitions/count: {} / {}", imageAndMaskPairs.getNumPartitions(), imageAndMaskPairs.count());

        JavaRDD<ColorMIPSearchResult> searchResults = imageAndMaskPairs.mapPartitions(imageAndMaskPairsItr -> {
            // the image should already be in memory so in each partition we only have to load the mask
            // for that we group the pairs of images and mask from the current partition and load every mask only once per partition
            Map<String, List<Tuple2<Tuple2<String, ImagePlus>, String>>> maskWithAllImagesFromCurrentPartitionMapping =
                    StreamSupport.stream(Spliterators.spliterator(imageAndMaskPairsItr, Integer.MAX_VALUE, 0), false)
                            .collect(Collectors.groupingBy(imageAndMaskPair -> imageAndMaskPair._2));
            return maskWithAllImagesFromCurrentPartitionMapping.entrySet()
                    .stream()
                    .flatMap(maskWithImagesEntry -> {
                        String maskPathname = maskWithImagesEntry.getKey();
                        LOG.info("Load mask image from {}", maskPathname);
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
                                    Preconditions.checkArgument(maskPathname.equals(maskWithImage._2)); // extra check to ensure the mask is the same as the one we loaded
                                    return new Tuple2<>(maskWithImage._1, new Tuple2<>(maskWithImage._2, maskImage));
                                });
                    })
                    .map((Tuple2<Tuple2<String, ImagePlus>, Tuple2<String, ImagePlus>> imageAndMaskPair) -> runImageComparison(imageAndMaskPair._1._1, imageAndMaskPair._1._2.getProcessor(), imageAndMaskPair._2._1, imageAndMaskPair._2._2.getProcessor(), maskThreshold))
                    .iterator();
        });

        JavaRDD<ColorMIPSearchResult> sortedSearchResultsRDD = searchResults.sortBy(ColorMIPSearchResult::getMatchingSlices, false, 1);
        LOG.info("Sorted search results partitions: {}", sortedSearchResultsRDD.getNumPartitions());

        List<ColorMIPSearchResult> sortedSearchResults = sortedSearchResultsRDD.collect();
        LOG.info("Returned {} results for {} masks", sortedSearchResults.size(), maskPathnames.size());

        return sortedSearchResults;
    }

    List<ColorMIPSearchResult> searchMaskFromFileInAllImages(String maskPathname, Integer maskThreshold) {
        LOG.info("Searching mask {} against {} libraries", maskPathname, imagePlusRDD.count());

        // for each image partition load the mask once and run the color depth comparison
        JavaRDD<ColorMIPSearchResult> searchResults = imagePlusRDD.mapPartitions(imagePathPairItr -> StreamSupport.stream(Spliterators.spliterator(imagePathPairItr, Integer.MAX_VALUE, 0), false)
                .map(imagePathPair -> {
                    // Cache mask object at the task level
                    Path maskPath = Paths.get(maskPathname);
                    ImagePlus maskImage ;
                    try {
                        maskImage = readImagePlus(maskPath.getFileName().toString(), maskPathname);
                    } catch (Exception e) {
                        LOG.error("Error loading {}", maskPathname, e);
                        throw new IllegalStateException("Error loading " + maskPathname, e);
                    }
                    return runImageComparison(imagePathPair._1, imagePathPair._2.getProcessor(), maskPathname, maskImage.getProcessor(), maskThreshold);
                })
                .iterator())
                ;
        LOG.info("Search results partitions: {}", searchResults.getNumPartitions());

        JavaRDD<ColorMIPSearchResult> sortedSearchResultsRDD = searchResults.sortBy(ColorMIPSearchResult::getMatchingSlices, false, 1);

        LOG.info("Sorted search results partitions: {}", sortedSearchResultsRDD.getNumPartitions());

        List<ColorMIPSearchResult> sortedSearchResults = sortedSearchResultsRDD.collect();

        LOG.info("Returned {} results for mask {}", sortedSearchResults.size(), maskPathname);

        return sortedSearchResults;
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

    void terminate() {
        sparkContext.close();
    }

}
