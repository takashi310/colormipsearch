package org.janelia.colormipsearch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.imageio.ImageIO;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import ij.ImagePlus;
import ij.io.Opener;
import ij.process.ImageProcessor;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

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

    private String outputPath;
    private Integer dataThreshold;
    private Integer xyShift;
    private boolean mirrorMask;
    private Double pixColorFluctuation;
    private Double pctPositivePixels;
    private transient final JavaSparkContext sparkContext;

    ColorMIPSearch(String appName,
                   String outputPath,
                   Integer dataThreshold, Double pixColorFluctuation, Integer xyShift,
                   boolean mirrorMask, Double pctPositivePixels) {
        this.outputPath =  outputPath;
        this.dataThreshold = dataThreshold;
        this.pixColorFluctuation = pixColorFluctuation;
        this.xyShift = xyShift;
        this.mirrorMask = mirrorMask;
        this.pctPositivePixels = pctPositivePixels;
        this.sparkContext = new JavaSparkContext(new SparkConf().setAppName(appName));
    }

    /**
     * Load provided image libraries into memory.
     * @param cdmips
     */
    JavaPairRDD<String, ImagePlus> loadMIPS(List<ColorDepthMIP> cdmips) {
        LOG.info("Load {} mips", cdmips.size());

        // This is a lot faster than using binaryFiles because 1) the paths are shuffled, 2) we use an optimized
        // directory listing stream which does not consider file sizes. As a bonus, it actually respects the parallelism
        // setting, unlike binaryFiles which ignores it unless you set other arcane settings like openCostInByte.
        JavaRDD<ColorDepthMIP> cdmipsRDD = sparkContext.parallelize(cdmips);
        LOG.info("cdmipsRDD {} items in {} partitions", cdmipsRDD.count(), cdmipsRDD.getNumPartitions());

        // This RDD is cached so that it can be reused to search with multiple masks
        JavaPairRDD<String, ImagePlus> cdmipImagesRDD = cdmipsRDD.mapToPair(cdmip -> {
            return new Tuple2<>(cdmip.id, readImagePlus(cdmip.id, cdmip.filepath));
        });

        LOG.info("cdmipImagesRDD {} images in {} partitions", cdmipImagesRDD.count(), cdmipImagesRDD.getNumPartitions());
        return cdmipImagesRDD;
    }

    private ImagePlus readImagePlus(String title, String filepath) {
        try {
            return readImagePlus(title, getImageFormat(filepath), new FileInputStream(filepath));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
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

    void  compareEveryMaskWithEveryLibrary(List<MIPImage> maskMIPS, List<MIPImage> libraryMIPS, Integer maskThreshold, int defaultPartitionSize) {
        LOG.info("Searching {} masks against {} libraries", maskMIPS.size(), libraryMIPS.size());

        List<List<MIPImage>> partitionedLibraries = partitionList(libraryMIPS, defaultPartitionSize);
        LOG.info("Split {} libraries into {} partitions", libraryMIPS.size(), partitionedLibraries.size());

        List<List<MIPImage>> partitionedMasks = partitionList(maskMIPS, defaultPartitionSize);
        LOG.info("Split {} masks into {} partitions", maskMIPS.size(), partitionedMasks.size());

        JavaRDD<List<MIPImage>> librariesRDD = sparkContext.parallelize(partitionedLibraries);
        LOG.info("Created RDD libraries and put {} items into {} partitions", librariesRDD.count(), librariesRDD.getNumPartitions());

        JavaRDD<List<MIPImage>> masksRDD = sparkContext.parallelize(partitionedMasks);
        LOG.info("Created RDD masks and put {} items into {} partitions", masksRDD.count(), masksRDD.getNumPartitions());

        JavaPairRDD<List<MIPImage>, List<MIPImage>> librariesMasksPairsRDD = librariesRDD.cartesian(masksRDD);
        LOG.info("Created {} library masks pairs in {} partitions", librariesMasksPairsRDD.count(), librariesMasksPairsRDD.getNumPartitions());

        JavaRDD<ColorMIPSearchResult> searchResults = librariesMasksPairsRDD.mapPartitions(librariesAndMasksItr -> {
            return StreamSupport.stream(Spliterators.spliterator(librariesAndMasksItr, Integer.MAX_VALUE, 0), true)
                    .flatMap(librariesAndMasks -> librariesAndMasks._1.stream()
                            .filter(libraryMIP -> new File(libraryMIP.filepath).exists())
                            .map(libraryMIP -> libraryMIP.withImage(readImagePlus(libraryMIP.id, libraryMIP.filepath)))
                            .flatMap(libraryMIPWithImage -> librariesAndMasks._2.stream()
                                    .filter(maskMIP -> new File(maskMIP.filepath).exists())
                                    .map(maskMIP -> maskMIP.withImage(readImagePlus(maskMIP.id, maskMIP.filepath)))
                                    .map(maskMIPWithImage -> new Tuple2<>(libraryMIPWithImage, maskMIPWithImage)))
                    )
                    .map((Tuple2<MIPImage, MIPImage> libraryAndMaskPair) -> runImageComparison(libraryAndMaskPair._1, libraryAndMaskPair._2, maskThreshold))
                    .iterator()
            ;
        });

        LOG.info("Found {} results in {} partitions", searchResults.count(), searchResults.getNumPartitions());

        // write results for each library
        LOG.info("Write results for each library item");
        writeAllSearchResults(searchResults, ResultGroupingCriteria.BY_LIBRARY);
        // write results for each mask
        LOG.info("Write results for each mask");
        writeAllSearchResults(searchResults, ResultGroupingCriteria.BY_MASK);
    }

    private <T> List<List<T>> partitionList(List<T> l, int partitionSize) {
        BiFunction<Tuple2<List<List<T>>, List<T>>, T, Tuple2<List<List<T>>, List<T>>> partitionAcumulator = (partitionResult, s) -> {
            List<T> currentPartition;
            if (partitionResult._2.size() == partitionSize) {
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
        return l.stream().reduce(
                new Tuple2<>(new ArrayList<>(), new ArrayList<>()),
                partitionAcumulator,
                (r1, r2) -> r2._1.stream().flatMap(p -> p.stream())
                        .map(s -> partitionAcumulator.apply(r1, s))
                        .reduce((first, second) -> second)
                        .orElse(r1))._1
                ;
    }

    private ColorMIPSearchResult runImageComparison(MIPImage libraryMIP, MIPImage patternMIP, Integer searchThreshold) {
        try {
            LOG.info("Compare library file {} with mask {} using threshold {}", libraryMIP,  patternMIP, searchThreshold);

            double pixfludub = pixColorFluctuation / 100;

            final ColorMIPMaskCompare cc = new ColorMIPMaskCompare(
                    patternMIP.image.getProcessor(),
                    searchThreshold,
                    mirrorMask,
                    null,
                    0,
                    mirrorMask,
                    dataThreshold,
                    pixfludub,
                    xyShift
            );
            ColorMIPMaskCompare.Output output = cc.runSearch(libraryMIP.image.getProcessor(), null);

            double pixThresdub = pctPositivePixels / 100;
            boolean isMatch = output.matchingPct > pixThresdub;

            return new ColorMIPSearchResult(patternMIP.id, patternMIP.filepath, libraryMIP.id, libraryMIP.filepath, output.matchingPixNum, output.matchingPct, isMatch, false);
        } catch (Throwable e) {
            LOG.info("Error comparing library file {} with mask {}", libraryMIP,  patternMIP, e);
            return new ColorMIPSearchResult(patternMIP.id, patternMIP.filepath, libraryMIP.id, libraryMIP.filepath, 0, 0, false, true);
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
        LOG.info("Grouped {} results into {} partitions using {} criteria", groupedSearchResults.count(), groupedSearchResults.getNumPartitions(), groupingCriteria);

        JavaPairRDD<String, List<ColorMIPSearchResult>> combinedSearchResults = groupedSearchResults.combineByKey(
                srForKey -> StreamSupport.stream(srForKey.spliterator(), true)
                        .sorted(Comparator.comparingInt(ColorMIPSearchResult::getMatchingSlices))
                        .collect(Collectors.toList()),
                (srForKeyList, srForKey) -> {
                    LOG.info("Merging {} elements with {} elements", srForKeyList.size(), Iterables.size(srForKey));
                    return Stream.concat(srForKeyList.stream(), StreamSupport.stream(srForKey.spliterator(), true))
                            .sorted(Comparator.comparingInt(ColorMIPSearchResult::getMatchingSlices))
                            .collect(Collectors.toList());
                },
                (sr1List, sr2List) -> {
                    LOG.info("Merging {} combined elements with {} combined elements", sr1List.size(), sr2List.size());
                    return Stream.concat(sr1List.stream(), sr2List.stream())
                            .sorted(Comparator.comparingInt(ColorMIPSearchResult::getMatchingSlices))
                            .collect(Collectors.toList());
               }
        );
        LOG.info("Combined {} results into {} partitions using {} criteria", groupedSearchResults.count(), groupedSearchResults.getNumPartitions(), groupingCriteria);

        combinedSearchResults.foreach(keyWithSearchResults -> {
            LOG.info("Write {} sorted results for {}", keyWithSearchResults._2.size(), keyWithSearchResults._1);
            FileOutputStream outputStream;
            File outputFile = new File(outputPath, keyWithSearchResults._1 + ".json");
            try {
                outputStream = new FileOutputStream(outputFile);
            } catch (FileNotFoundException e) {
                LOG.error("Error opening the outputfile {}", outputPath, e);
                return;
            }
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonGenerator gen = mapper.getFactory().createGenerator(outputStream, JsonEncoding.UTF8);
                gen.useDefaultPrettyPrinter();
                gen.writeStartObject();
                gen.writeArrayFieldStart("results");
                for (ColorMIPSearchResult sr : keyWithSearchResults._2) {
                    gen.writeObject(sr);
                }
                gen.writeEndArray();
                gen.writeEndObject();
                gen.flush();
            } catch (IOException e) {
                LOG.error("Error writing json output for {} outputfile {}", keyWithSearchResults._2, outputPath, e);
            } finally {
                try {
                    outputStream.close();
                } catch (IOException ignore) {
                }
            }

        });
    }
    
    void terminate() {
        sparkContext.close();
    }

}
