package org.janelia.colormipsearch;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import ij.ImagePlus;
import ij.io.Opener;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.DataInputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;

/**
 * Perform color depth mask search on a Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SparkMaskSearch implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(SparkMaskSearch.class);

//    private static BufferedImage readTiff(PortableDataStream stream) throws IOException {
//        ByteArraySeekableStream seekStream = new ByteArraySeekableStream(stream.toArray());
//        TIFFDecodeParam decodeParam = new TIFFDecodeParam();
//        decodeParam.setDecodePaletteAsShorts(true);
//        ParameterBlock params = new ParameterBlock();
//        params.add(seekStream);
//        RenderedOp image1 = JAI.create("tiff", params);
//        return image1.getAsBufferedImage();
//    }

    private ImagePlus readTiffToImagePlus(PortableDataStream stream) throws Exception {

        // Attempt using BioFormats importer failed to give a 32 bit composite image:
//        IRandomAccess ira = new ByteArrayHandle(stream.toArray());
//        String id = UUID.randomUUID().toString();
//        Location.mapFile(id, ira);
//        ImporterOptions options = new ImporterOptions();
//        options.setId(id);
//        options.setAutoscale(false);
//        options.setColorMode(ImporterOptions.COLOR_MODE_COMPOSITE);
//        ImagePlus[] imps = BF.openImagePlus(options);
//        ImagePlus mask = imps[0];

        // But using ImageJ works:
        Opener opener = new Opener();
        try (DataInputStream dis = stream.open()) {
            ImagePlus mask = opener.openTiff(dis, "search");
            return mask;
        }
    }

    private MaskSearchResult search(String filepath, ImagePlus image, byte[] maskBytes) throws Exception {

        if (image==null) {
            log.error("Problem loading search image: {}", filepath);
            return new MaskSearchResult(filepath, 0, 0, false);
        }

        ImagePlus mask = new Opener().deserialize(maskBytes);

        ColorMIPMaskCompare.Parameters params = new ColorMIPMaskCompare.Parameters();
        params.maskImage = mask;
        params.searchImage = image;
        ColorMIPMaskCompare search = new ColorMIPMaskCompare();
        ColorMIPMaskCompare.Output output = search.runSearch(params);

        return new MaskSearchResult(filepath, output.matchingSlices, output.matchingSlicesPct, output.isMatch);
    }

    /**
     * Perform the search.
     * @param imagesFilepath
     * @param maskFilename
     * @return
     * @throws Exception
     */
    public Collection<MaskSearchResult> search(String imagesFilepath, String maskFilename) throws Exception {

        log.info("Searching image archive at: {}", imagesFilepath);

        JavaSparkContext context = null;
        List<MaskSearchResult> results = null;

        try {
            byte[] maskBytes = Files.readAllBytes(Paths.get(maskFilename));

            SparkConf conf = new SparkConf().setAppName(SparkMaskSearch.class.getName());
            context = new JavaSparkContext(conf);

            log.info("defaultMinPartitions: {}", context.defaultMinPartitions());
            log.info("defaultParallelism: {}", context.defaultParallelism());

            JavaPairRDD<String, PortableDataStream> filesRdd = context.binaryFiles(imagesFilepath);
            log.info("binaryFiles.numPartitions: {}", filesRdd.getNumPartitions());

            JavaRDD<MaskSearchResult> resultRdd = filesRdd
                    .mapToPair(pair -> new Tuple2<>(pair._1, readTiffToImagePlus(pair._2)))
                    .map(pair -> search(pair._1, pair._2, maskBytes))
                    .sortBy(result -> result.getMatchingSlices(), false, 16);

            results = resultRdd.collect();
        }
        finally {
            if (context!=null) context.stop();
        }

        return results;
    }

    public static class Args {

        @Parameter(names = {"--mask", "-m"}, description = "TIFF file to use as the search mask")
        private String maskFilename;

        @Parameter(names = {"--imageDir", "-i"}, description = "TIFF files to search")
        private String imageDir;

    }

    public static void main(String[] argv) throws Exception {

        Args args = new Args();
        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(argv);

        SparkMaskSearch sparkMaskSearch = new SparkMaskSearch();
        Collection<MaskSearchResult> results = sparkMaskSearch.search(
                args.imageDir,
                args.maskFilename);

        for (MaskSearchResult result : results) {
            if (result.isMatch()) {
                log.info("{} - {}", result.getMatchingSlicesPct(), result.getFilepath());
            }
        }
    }

}
