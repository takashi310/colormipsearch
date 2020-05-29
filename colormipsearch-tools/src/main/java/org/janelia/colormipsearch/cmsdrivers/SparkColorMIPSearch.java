package org.janelia.colormipsearch.cmsdrivers;

import java.io.File;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.janelia.colormipsearch.tools.ColorMIPSearch;
import org.janelia.colormipsearch.tools.ColorMIPSearchResult;
import org.janelia.colormipsearch.tools.MIPImage;
import org.janelia.colormipsearch.tools.MIPInfo;
import org.janelia.colormipsearch.tools.MIPsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SparkColorMIPSearch implements ColorMIPSearchDriver {

    private static final Logger LOG = LoggerFactory.getLogger(SparkColorMIPSearch.class);

    private final ColorMIPSearch colorMIPSearch;
    private transient final JavaSparkContext sparkContext;

    public SparkColorMIPSearch(String appName,
                               ColorMIPSearch colorMIPSearch) {
        this.colorMIPSearch = colorMIPSearch;
        this.sparkContext = new JavaSparkContext(new SparkConf().setAppName(appName));
    }

    @Override
    public List<ColorMIPSearchResult> findAllColorDepthMatches(List<MIPInfo> maskMIPS, List<MIPInfo> libraryMIPS) {
        LOG.info("Searching {} masks against {} libraries", maskMIPS.size(), libraryMIPS.size());

        long nlibraries = libraryMIPS.size();
        long nmasks = maskMIPS.size();

        JavaRDD<MIPImage> librariesRDD = sparkContext.parallelize(libraryMIPS)
                .filter(mip -> new File(mip.getImagePath()).exists())
                .map(mip -> MIPsUtils.loadMIP(mip));
        LOG.info("Created RDD libraries and put {} items into {} partitions", nlibraries, librariesRDD.getNumPartitions());

        JavaRDD<MIPInfo> masksRDD = sparkContext.parallelize(maskMIPS)
                .filter(mip -> new File(mip.getImagePath()).exists());
        LOG.info("Created RDD masks and put {} items into {} partitions", nmasks, masksRDD.getNumPartitions());

        JavaPairRDD<MIPImage, MIPInfo> librariesMasksPairsRDD = librariesRDD.cartesian(masksRDD);
        LOG.info("Created {} library masks pairs in {} partitions", nmasks * nlibraries, librariesMasksPairsRDD.getNumPartitions());

        JavaPairRDD<MIPInfo, List<ColorMIPSearchResult>> allSearchResultsPartitionedByMaskMIP = librariesMasksPairsRDD
                .groupBy(lms -> lms._2) // group by mask
                .mapPartitions(mlItr -> StreamSupport.stream(Spliterators.spliterator(mlItr, Integer.MAX_VALUE, 0), false)
                        .map(mls -> {
                            MIPImage maskMIP = MIPsUtils.loadMIP(mls._1);
                            List<ColorMIPSearchResult> srsByMask = StreamSupport.stream(mls._2.spliterator(), false)
                                    .map(lmPair -> colorMIPSearch.runImageComparison(lmPair._1, maskMIP))
                                    .filter(srByMask -> srByMask.isMatch() || srByMask.isError())
                                    .sorted(colorMIPSearch.getColorMIPSearchResultComparator())
                                    .collect(Collectors.toList());
                            return new Tuple2<>(mls._1, srsByMask);
                        })
                        .iterator())
                .mapToPair(p -> p);
        LOG.info("Created RDD search results for  all {} library-mask pairs in all {} partitions", nmasks * nlibraries, allSearchResultsPartitionedByMaskMIP.getNumPartitions());

        // write results for each mask
        return allSearchResultsPartitionedByMaskMIP.flatMapToPair(srByMask -> srByMask._2.stream().map(sr -> new Tuple2<>(srByMask._1, sr)).iterator())
                .map(mipWithSr -> mipWithSr._2)
                .collect();
    }

    @Override
    public void terminate() {
        sparkContext.close();
    }

}
