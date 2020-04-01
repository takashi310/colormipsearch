package org.janelia.colormipsearch;

import java.io.File;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

    private transient final JavaSparkContext sparkContext;

    SparkColorMIPSearch(String appName,
                        String gradientMasksPath,
                        Integer dataThreshold,
                        Integer maskThreshold,
                        Double pixColorFluctuation,
                        Integer xyShift,
                        int negativeRadius,
                        boolean mirrorMask,
                        Double pctPositivePixels) {
        super(gradientMasksPath, dataThreshold, maskThreshold, pixColorFluctuation, xyShift, negativeRadius, mirrorMask, pctPositivePixels);
        this.sparkContext = new JavaSparkContext(new SparkConf().setAppName(appName));
    }

    @Override
    List<ColorMIPSearchResult> findAllColorDepthMatches(List<MIPInfo> maskMIPS, List<MIPInfo> libraryMIPS) {
        LOG.info("Searching {} masks against {} libraries", maskMIPS.size(), libraryMIPS.size());

        long nlibraries = libraryMIPS.size();
        long nmasks = maskMIPS.size();

        JavaRDD<Tuple2<MIPImage, MIPImage>> librariesRDD = sparkContext.parallelize(libraryMIPS)
                .filter(mip -> new File(mip.imagePath).exists())
                .map(mip -> new Tuple2<>(loadMIP(mip), loadGradientMIP(mip)))
                ;
        LOG.info("Created RDD libraries and put {} items into {} partitions", nlibraries, librariesRDD.getNumPartitions());

        JavaRDD<MIPInfo> masksRDD = sparkContext.parallelize(maskMIPS)
                .filter(mip -> new File(mip.imagePath).exists())
                ;
        LOG.info("Created RDD masks and put {} items into {} partitions", nmasks, masksRDD.getNumPartitions());

        JavaPairRDD<Tuple2<MIPImage, MIPImage>, MIPInfo> librariesMasksPairsRDD = librariesRDD.cartesian(masksRDD);
        LOG.info("Created {} library masks pairs in {} partitions", nmasks * nlibraries, librariesMasksPairsRDD.getNumPartitions());

        JavaPairRDD<MIPInfo, List<ColorMIPSearchResult>> allSearchResultsPartitionedByMaskMIP = librariesMasksPairsRDD
                .groupBy(lms -> lms._2) // group by mask
                .mapPartitions(mlItr -> StreamSupport.stream(Spliterators.spliterator(mlItr, Integer.MAX_VALUE, 0), false)
                        .map(mls -> {
                            MIPImage maskMIP = loadMIP(mls._1);
                            MIPImage gradientMask = loadGradientMIP(mls._1);
                            List<ColorMIPSearchResult> srsByMask = StreamSupport.stream(mls._2.spliterator(), false)
                                    .map(lmPair -> {
                                        ColorMIPSearchResult sr = runImageComparison(lmPair._1._1, maskMIP);
                                        return applyGradientAreaAdjustment(sr, lmPair._1._1, lmPair._1._2, maskMIP, gradientMask);
                                    })
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
        return allSearchResultsPartitionedByMaskMIP.flatMapToPair(srByMask -> srByMask._2.stream().map(sr -> new Tuple2<>(srByMask._1, sr)).iterator())
                .map(mipWithSr -> mipWithSr._2)
                .collect();
    }

    @Override
    void terminate() {
        sparkContext.close();
    }

}
