package org.janelia.colormipsearch.cmd;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.commons.collections4.CollectionUtils;
import org.janelia.colormipsearch.cds.ColorDepthSearchAlgorithmProvider;
import org.janelia.colormipsearch.cds.ColorDepthSearchAlgorithmProviderFactory;
import org.janelia.colormipsearch.cds.ColorMIPSearch;
import org.janelia.colormipsearch.cds.PixelMatchScore;
import org.janelia.colormipsearch.cmd.cdsprocess.ColorMIPSearchProcessor;
import org.janelia.colormipsearch.cmd.cdsprocess.LocalColorMIPSearchProcessor;
import org.janelia.colormipsearch.cmd.cdsprocess.SparkColorMIPSearchProcessor;
import org.janelia.colormipsearch.dataio.CDMIPsReader;
import org.janelia.colormipsearch.dataio.CDSSessionWriter;
import org.janelia.colormipsearch.dataio.NeuronMatchesWriter;
import org.janelia.colormipsearch.dataio.PartitionedNeuronMatchesWriter;
import org.janelia.colormipsearch.dataio.db.DBCDMIPsReader;
import org.janelia.colormipsearch.dataio.db.DBCDSSessionWriter;
import org.janelia.colormipsearch.dataio.db.DBCDScoresOnlyWriter;
import org.janelia.colormipsearch.dataio.db.DBNeuronMatchesWriter;
import org.janelia.colormipsearch.dataio.fs.JSONCDMIPsReader;
import org.janelia.colormipsearch.dataio.fs.JSONCDSSessionWriter;
import org.janelia.colormipsearch.dataio.fs.JSONNeuronMatchesWriter;
import org.janelia.colormipsearch.imageprocessing.ImageRegionDefinition;
import org.janelia.colormipsearch.model.AbstractNeuronEntity;
import org.janelia.colormipsearch.model.CDMatchEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command that runs the Color Depth Search.
 */
class ColorDepthSearchCmd extends AbstractCmd {

    private static final Logger LOG = LoggerFactory.getLogger(ColorDepthSearchCmd.class);

    @Parameters(commandDescription = "Color depth search for a batch of MIPs")
    static class ColorDepthSearchArgs extends AbstractColorDepthMatchArgs {

        @Parameter(names = {"--mips-storage"},
                description = "Specifies MIPs storage")
        StorageType mipsStorage = StorageType.DB;

        @Parameter(names = {"--update-matches"},
                description = "If set a new color depth search run will always create new results; " +
                        "the default behavior is to update entries that match same images", arity = 0)
        boolean updateExistingMatches = false;

        @Parameter(names = {"--masks", "-m"}, required = true, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "Image file(s) to use as the search masks")
        List<ListArg> masksInputs;

        @Parameter(names = {"--masks-index"}, description = "Mask file(s) start index")
        long masksStartIndex;

        @Parameter(names = {"--masks-length"}, description = "Mask file(s) length")
        int masksLength;

        @Parameter(names = {"--targets", "-i"}, required = true, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "Comma-delimited list of JSON configs containing images to search")
        List<ListArg> targetsInputs;

        @Parameter(names = {"--targets-index"}, description = "Input image file(s) start index")
        long targetsStartIndex;

        @Parameter(names = {"--targets-length"}, description = "Input image file(s) length")
        int targetsLength;

        ColorDepthSearchArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }

    }

    private final ColorDepthSearchArgs args;
    private final Supplier<Long> cacheSizeSupplier;
    private final boolean useSpark;
    private final ObjectMapper mapper;

    ColorDepthSearchCmd(String commandName,
                        CommonArgs commonArgs,
                        Supplier<Long> cacheSizeSupplier,
                        boolean useSpark) {
        super(commandName);
        this.args = new ColorDepthSearchArgs(commonArgs);
        this.cacheSizeSupplier = cacheSizeSupplier;
        this.useSpark = useSpark;
        this.mapper = new ObjectMapper()
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        ;
    }

    @Override
    ColorDepthSearchArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        // initialize the cache
        CachedMIPsUtils.initializeCache(cacheSizeSupplier.get());
        // perform color depth search for all masks against all targets
        runColorDepthSearch();
    }

    private <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity> void runColorDepthSearch() {
        CDMIPsReader cdmiPsReader = getCDMipsReader();
        ColorMIPSearchProcessor<M, T> colorMIPSearchProcessor;
        ColorDepthSearchAlgorithmProvider<PixelMatchScore> cdsAlgorithmProvider;
        ImageRegionDefinition excludedRegions = args.getRegionGeneratorForTextLabels();
        cdsAlgorithmProvider = ColorDepthSearchAlgorithmProviderFactory.createPixMatchCDSAlgorithmProvider(
                args.mirrorMask,
                args.dataThreshold,
                args.pixColorFluctuation,
                args.xyShift,
                excludedRegions
        );
        ColorMIPSearch colorMIPSearch = new ColorMIPSearch(args.pctPositivePixels, args.maskThreshold, cdsAlgorithmProvider);
        @SuppressWarnings("unchecked")
        List<M> maskMips = (List<M>) readMIPs(cdmiPsReader,
                args.masksInputs,
                args.masksStartIndex, args.masksLength,
                args.maskMIPsFilter);
        @SuppressWarnings("unchecked")
        List<T> targetMips = (List<T>) readMIPs(cdmiPsReader,
                args.targetsInputs,
                args.targetsStartIndex, args.targetsLength,
                args.libraryMIPsFilter);
        if (maskMips.isEmpty() || targetMips.isEmpty()) {
            LOG.info("Nothing to do for {} masks and {} targets", maskMips.size(), targetMips.size());
            return;
        }
        // save CDS parameters
        Number cdsRunId = getCDSSessionWriter().createSession(
                args.masksInputs.stream().map(ListArg::asDataSourceParam).collect(Collectors.toList()),
                args.targetsInputs.stream().map(ListArg::asDataSourceParam).collect(Collectors.toList()),
                colorMIPSearch.getCDSParameters());
        if (useSpark) {
            colorMIPSearchProcessor = new SparkColorMIPSearchProcessor<>(
                    cdsRunId,
                    args.appName,
                    colorMIPSearch,
                    args.processingPartitionSize
            );
        } else {
            colorMIPSearchProcessor = new LocalColorMIPSearchProcessor<>(
                    cdsRunId,
                    colorMIPSearch,
                    args.processingPartitionSize,
                    CmdUtils.createCmdExecutor(args.commonArgs)
            );
        }
        try {
            List<CDMatchEntity<M, T>> cdsResults = colorMIPSearchProcessor.findAllColorDepthMatches(maskMips, targetMips);
            NeuronMatchesWriter<CDMatchEntity<M, T>> cdsResultsWriter = getCDSMatchesWriter();
            cdsResultsWriter.write(cdsResults);
        } finally {
            colorMIPSearchProcessor.terminate();
        }
    }

    private CDMIPsReader getCDMipsReader() {
        if (args.mipsStorage == StorageType.DB) {
            return new DBCDMIPsReader(getConfig());
        } else {
            return new JSONCDMIPsReader(mapper);
        }
    }

    private CDSSessionWriter getCDSSessionWriter() {
        if (args.mipsStorage == StorageType.DB) {
            return new DBCDSSessionWriter(getConfig());
        } else {
            return new JSONCDSSessionWriter(
                    args.getOutputDir(),
                    mapper);
        }
    }

    private <M extends AbstractNeuronEntity, T extends AbstractNeuronEntity> NeuronMatchesWriter<CDMatchEntity<M, T>>
    getCDSMatchesWriter() {
        if (args.commonArgs.resultsStorage == StorageType.DB) {
            if (args.updateExistingMatches) {
                // if a match exists update the scoress
                // since this writes items one at a time - partition and process partitions in parallel
                return new PartitionedNeuronMatchesWriter<>(
                        new DBCDScoresOnlyWriter<>(getConfig()),
                        args.processingPartitionSize,
                        true
                );
            } else {
                // always create new matches
                return new DBNeuronMatchesWriter<>(getConfig());
            }
        } else {
            return new JSONNeuronMatchesWriter<>(
                    args.commonArgs.noPrettyPrint ? mapper.writer() : mapper.writerWithDefaultPrettyPrinter(),
                    AbstractNeuronEntity::getMipId, // group results by neuron MIP ID
                    Comparator.comparingDouble(m -> -(((CDMatchEntity<?,?>) m).getMatchingPixels())), // descending order by matching pixels
                    args.getPerMaskDir(),
                    args.getPerTargetDir()
            );
        }
    }

    private List<? extends AbstractNeuronEntity> readMIPs(CDMIPsReader mipsReader,
                                                          List<ListArg> mipsArg, long startIndexArg, int length,
                                                          Set<String> filter) {
        long startIndex = startIndexArg > 0 ? startIndexArg : 0;
        List<? extends AbstractNeuronEntity> allMips = mipsArg.stream()
                .flatMap(libraryInput -> mipsReader.readMIPs(ListArg.asDataSourceParam(libraryInput)).stream())
                .filter(neuronMetadata -> CollectionUtils.isEmpty(filter) ||
                        filter.contains(neuronMetadata.getPublishedName().toLowerCase()) ||
                        filter.contains(neuronMetadata.getMipId()))
                .skip(startIndex)
                .collect(Collectors.toList());
        return length > 0 && length < allMips.size()
                ? allMips.subList(0, length)
                : allMips;
    }
}
