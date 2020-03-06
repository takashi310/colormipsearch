package org.janelia.colormipsearch;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.Iterables;

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
class SparkColorMIPSearch extends ColorMIPSearch {

    private static final Logger LOG = LoggerFactory.getLogger(SparkColorMIPSearch.class);

    private enum ResultGroupingCriteria {
        BY_LIBRARY(ColorMIPSearchResult::perLibraryMetadata),
        BY_MASK(ColorMIPSearchResult::perMaskMetadata)
        ;

        private Function<ColorMIPSearchResult, ColorMIPSearchResultMetadata> transformation;

        private ResultGroupingCriteria(Function<ColorMIPSearchResult, ColorMIPSearchResultMetadata> transformation) {
            this.transformation = transformation;
        }
    }

    private transient final JavaSparkContext sparkContext;

    SparkColorMIPSearch(String appName,
                        String gradientMasksPath,
                        String outputPath,
                        Integer dataThreshold,
                        Integer maskThreshold,
                        Double pixColorFluctuation,
                        Integer xyShift,
                        int negativeRadius,
                        boolean mirrorMask,
                        Double pctPositivePixels) {
        super(gradientMasksPath, outputPath, dataThreshold, maskThreshold, pixColorFluctuation, xyShift, negativeRadius, mirrorMask, pctPositivePixels);
        this.sparkContext = new JavaSparkContext(new SparkConf().setAppName(appName));
    }

    @Override
    void  compareEveryMaskWithEveryLibrary(List<MIPInfo> maskMIPS, List<MIPInfo> libraryMIPS) {
        LOG.info("Searching {} masks against {} libraries", maskMIPS.size(), libraryMIPS.size());

        long nlibraries = libraryMIPS.size();
        long nmasks = maskMIPS.size();

        JavaRDD<MIPWithPixels> librariesRDD = sparkContext.parallelize(libraryMIPS)
                .filter(mip -> new File(mip.imageFilepath).exists())
                .map(this::loadMIP)
                ;
        LOG.info("Created RDD libraries and put {} items into {} partitions", nlibraries, librariesRDD.getNumPartitions());

        JavaRDD<MIPInfo> masksRDD = sparkContext.parallelize(maskMIPS)
                .filter(mip -> new File(mip.imageFilepath).exists())
                ;
        LOG.info("Created RDD masks and put {} items into {} partitions", nmasks, masksRDD.getNumPartitions());

        JavaPairRDD<MIPWithPixels, MIPInfo> librariesMasksPairsRDD = librariesRDD.cartesian(masksRDD);
        LOG.info("Created {} library masks pairs in {} partitions", nmasks * nlibraries, librariesMasksPairsRDD.getNumPartitions());

        JavaPairRDD<MIPInfo, List<ColorMIPSearchResult>> allSearchResultsPartitionedByMaskMIP = librariesMasksPairsRDD
                .groupBy(lms -> lms._2) // group by mask
                .mapPartitions(mlItr -> StreamSupport.stream(Spliterators.spliterator(mlItr, Integer.MAX_VALUE, 0), false)
                        .map(mls -> {
                            MIPWithPixels maskMIP = loadMIP(mls._1);
                            List<ColorMIPSearchResult> srsByMask = StreamSupport.stream(mls._2.spliterator(), false)
                                    .map(lmPair -> runImageComparison(lmPair._1, maskMIP))
                                    .filter(srByMask ->srByMask.isMatch() || srByMask.isError())
                                    .sorted(getColorMIPSearchComparator())
                                    .collect(Collectors.toList())
                                    ;
                            return new Tuple2<>(mls._1, srsByMask);
                        })
                        .iterator())
                .mapToPair(p -> p)
                ;
        LOG.info("Created RDD search results fpr  all {} library-mask pairs in all {} partitions", nmasks * nlibraries, allSearchResultsPartitionedByMaskMIP.getNumPartitions());

        // write results for each mask
        JavaPairRDD<MIPInfo, List<ColorMIPSearchResult>> matchingSearchResultsByMask = allSearchResultsPartitionedByMaskMIP
                .mapToPair(srByMask -> new Tuple2<>(srByMask._1, srByMask._2.stream()
                        .filter(ColorMIPSearchResult::isMatch)
                        .sorted(getColorMIPSearchComparator())
                        .collect(Collectors.toList())));

        matchingSearchResultsByMask.foreach(srByMask -> writeSearchResults(
                srByMask._1.id,
                srByMask._2.stream()
                        .map(ResultGroupingCriteria.BY_MASK.transformation)
                        .collect(Collectors.toList())));

        // write results for each library now
        writeAllSearchResults(matchingSearchResultsByMask.flatMap(srByLibraryMIP -> srByLibraryMIP._2.iterator()), ResultGroupingCriteria.BY_LIBRARY);

        // check for errors
        Map<MIPInfo, List<ColorMIPSearchResult>> errorSearchResultsByMaskMIP = allSearchResultsPartitionedByMaskMIP
                .mapToPair(srByMask -> new Tuple2<>(srByMask._1, srByMask._2.stream().filter(ColorMIPSearchResult::isError).collect(Collectors.toList())))
                .filter(srByMask -> !srByMask._2.isEmpty())
                .collectAsMap()
                ;
        LOG.error("Errors found for the following searches: {}", errorSearchResultsByMaskMIP);
    }

    private void writeAllSearchResults(JavaRDD<ColorMIPSearchResult> searchResults, ResultGroupingCriteria groupingCriteria) {
        JavaPairRDD<String, Iterable<ColorMIPSearchResult>> groupedSearchResults = searchResults.groupBy(sr -> {
            switch (groupingCriteria) {
                case BY_MASK:
                    return sr.getMaskId();
                case BY_LIBRARY:
                    return sr.getLibraryId();
                default:
                    throw new IllegalArgumentException("Invalid grouping criteria");
            }
        });
        LOG.info("Finished grouping into {} partitions using {} criteria", groupedSearchResults.getNumPartitions(), groupingCriteria);

        JavaPairRDD<String, List<ColorMIPSearchResult>> combinedSearchResults = groupedSearchResults.combineByKey(
                srForKey -> {
                    LOG.info("Combine and sort {} elements", Iterables.size(srForKey));
                    return StreamSupport.stream(srForKey.spliterator(), true)
                            .sorted(getColorMIPSearchComparator())
                            .collect(Collectors.toList());
                },
                (srForKeyList, srForKey) -> {
                    LOG.info("Merging {} elements with {} elements", srForKeyList.size(), Iterables.size(srForKey));
                    return Stream.concat(srForKeyList.stream(), StreamSupport.stream(srForKey.spliterator(), true))
                            .sorted(getColorMIPSearchComparator())
                            .collect(Collectors.toList());
                },
                (sr1List, sr2List) -> {
                    LOG.info("Merging {} combined elements with {} combined elements", sr1List.size(), sr2List.size());
                    return Stream.concat(sr1List.stream(), sr2List.stream())
                            .sorted(getColorMIPSearchComparator())
                            .collect(Collectors.toList());
                })
                ;
        LOG.info("Finished combining all results by key in {} partitions using {} criteria", combinedSearchResults.getNumPartitions(), groupingCriteria);

        combinedSearchResults
                .foreach(keyWithSearchResults -> writeSearchResults(
                        keyWithSearchResults._1,
                        keyWithSearchResults._2.stream()
                                .map(groupingCriteria.transformation)
                                .collect(Collectors.toList())));
        LOG.info("Finished writing the search results by {}", groupingCriteria);
    }

    @Override
    void terminate() {
        sparkContext.close();
    }

}
