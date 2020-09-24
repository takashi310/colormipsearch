package org.janelia.colormipsearch.cmsdrivers;

import java.io.Serializable;
import java.util.List;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.janelia.colormipsearch.api.cdmips.MIPImage;
import org.janelia.colormipsearch.api.cdmips.MIPMetadata;
import org.janelia.colormipsearch.api.cdmips.MIPsUtils;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPMatchScore;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearch;
import org.janelia.colormipsearch.api.cdsearch.ColorMIPSearchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SparkColorMIPSearch implements ColorMIPSearchDriver, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(SparkColorMIPSearch.class);

    private final ColorMIPSearch colorMIPSearch;
    private final List<String> gradientsLocations;
    private final MappingFunction<String, String> gradientVariantSuffixMapping;
    private final List<String> zgapMasksLocations;
    private final MappingFunction<String, String> zgapMaskVariantSuffixMapping;
    private transient final JavaSparkContext sparkContext;

    public SparkColorMIPSearch(String appName,
                               ColorMIPSearch colorMIPSearch,
                               List<String> gradientsLocations,
                               MappingFunction<String, String> gradientVariantSuffixMapping,
                               List<String> zgapMasksLocations,
                               MappingFunction<String, String> zgapMaskVariantSuffixMapping) {
        this.colorMIPSearch = colorMIPSearch;
        this.gradientsLocations = gradientsLocations;
        this.gradientVariantSuffixMapping = gradientVariantSuffixMapping;
        this.zgapMasksLocations = zgapMasksLocations;
        this.zgapMaskVariantSuffixMapping = zgapMaskVariantSuffixMapping;
        this.sparkContext = new JavaSparkContext(new SparkConf().setAppName(appName));
    }

    @Override
    public List<ColorMIPSearchResult> findAllColorDepthMatches(List<MIPMetadata> maskMIPS, List<MIPMetadata> libraryMIPS) {
        LOG.info("Searching {} masks against {} libraries", maskMIPS.size(), libraryMIPS.size());

        long nlibraries = libraryMIPS.size();
        long nmasks = maskMIPS.size();

        JavaRDD<MIPImage> librariesRDD = sparkContext.parallelize(libraryMIPS)
                .filter(MIPsUtils::exists)
                .map(MIPsUtils::loadMIP);
        LOG.info("Created RDD libraries and put {} items into {} partitions", nlibraries, librariesRDD.getNumPartitions());

        JavaRDD<MIPMetadata> masksRDD = sparkContext.parallelize(maskMIPS)
                .filter(MIPsUtils::exists);
        LOG.info("Created RDD masks and put {} items into {} partitions", nmasks, masksRDD.getNumPartitions());

        JavaPairRDD<MIPMetadata, MIPImage> masksLibrariesPairsRDD = masksRDD.cartesian(librariesRDD);
        LOG.info("Created {} library masks pairs in {} partitions", nmasks * nlibraries, masksLibrariesPairsRDD.getNumPartitions());

        JavaPairRDD<MIPMetadata, List<ColorMIPSearchResult>> allSearchResultsPartitionedByMaskMIP = masksLibrariesPairsRDD
                .groupBy(lms -> lms._1) // group by mask
                .mapPartitions(mlItr -> StreamSupport.stream(Spliterators.spliterator(mlItr, Integer.MAX_VALUE, 0), false)
                        .map(mls -> {
                            MIPImage maskImage = MIPsUtils.loadMIP(mls._1);
//                            ColorDepthSearchAlgorithm<ColorMIPMatchScore> maskColorDepthSearch = colorMIPSearch.createMaskColorDepthSearch(maskImage, null);
//                            Set<String> requiredVariantTypes = maskColorDepthSearch.getRequiredTargetVariantTypes();
                            List<ColorMIPSearchResult> srsByMask = StreamSupport.stream(mls._2.spliterator(), false)
                                    .map(maskLibraryPair -> {
                                        MIPImage libraryImage = maskLibraryPair._2;
//                                        Map<String, Supplier<ImageArray>> variantImageSuppliers = new HashMap<>();
//                                        if (requiredVariantTypes.contains("gradient")) {
//                                            variantImageSuppliers.put("gradient", () -> {
//                                                MIPImage gradientImage = CachedMIPsUtils.loadMIP(MIPsUtils.getMIPVariantInfo(
//                                                        MIPsUtils.getMIPMetadata(libraryImage),
//                                                        "gradient",
//                                                        gradientsLocations,
//                                                        gradientVariantSuffixMapping));
//                                                return MIPsUtils.getImageArray(gradientImage);
//                                            });
//                                        }
//                                        if (requiredVariantTypes.contains("zgap")) {
//                                            variantImageSuppliers.put("zgap", () -> {
//                                                MIPImage libraryZGapMaskImage = CachedMIPsUtils.loadMIP(MIPsUtils.getMIPVariantInfo(
//                                                        MIPsUtils.getMIPMetadata(libraryImage),
//                                                        "zgap",
//                                                        zgapMasksLocations,
//                                                        zgapMaskVariantSuffixMapping));
//                                                return MIPsUtils.getImageArray(libraryZGapMaskImage);
//                                            });
//                                        }
//                                        ColorMIPMatchScore colorMIPMatchScore = maskColorDepthSearch.calculateMatchingScore(
//                                                MIPsUtils.getImageArray(libraryImage),
//                                                variantImageSuppliers);
                                        ColorMIPMatchScore colorMIPMatchScore = new ColorMIPMatchScore(0, 0, null);
                                        boolean isMatch = colorMIPSearch.isMatch(colorMIPMatchScore);
                                        return new ColorMIPSearchResult(
                                                MIPsUtils.getMIPMetadata(maskImage),
                                                MIPsUtils.getMIPMetadata(libraryImage),
                                                colorMIPMatchScore,
                                                isMatch,
                                                false);
                                    })
                                    .filter(srByMask -> srByMask.isMatch() || srByMask.hasErrors())
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
