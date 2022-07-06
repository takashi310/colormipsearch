package org.janelia.colormipsearch.cmd;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.cds.ColorDepthSearchAlgorithmProvider;
import org.janelia.colormipsearch.cds.ColorDepthSearchAlgorithmProviderFactory;
import org.janelia.colormipsearch.cds.ShapeMatchScore;
import org.janelia.colormipsearch.cmd.io.CDMatchesReader;
import org.janelia.colormipsearch.cmd.io.IOUtils;
import org.janelia.colormipsearch.cmd.io.JSONFileCDMatchesReader;
import org.janelia.colormipsearch.imageprocessing.ImageArray;
import org.janelia.colormipsearch.imageprocessing.ImageRegionDefinition;
import org.janelia.colormipsearch.mips.NeuronMIPUtils;
import org.janelia.colormipsearch.model.AbstractNeuronMetadata;
import org.janelia.colormipsearch.model.CDSMatch;
import org.janelia.colormipsearch.model.EMNeuronMetadata;
import org.janelia.colormipsearch.model.FileData;
import org.janelia.colormipsearch.model.LMNeuronMetadata;
import org.janelia.colormipsearch.results.ItemsHandling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CalculateGradientScoresCmd extends AbstractCmd {

    private static final Logger LOG = LoggerFactory.getLogger(ColorDepthSearchCmd.class);

    @Parameters(commandDescription = "Color depth search for a batch of MIPs")
    static class GradientScoresArgs extends AbstractColorDepthMatchArgs {

        @Parameter(names = {"--matchesDir", "-md"}, required = true, variableArity = true,
                converter = ListArg.ListArgConverter.class,
                description = "Argument containing matches resulted from a color depth search process. " +
                        "This can be a directory of JSON files, a list of specific JSON files or some `DB selector`")
        List<ListArg> matches;

        @Parameter(names = {"--nBestLines"},
                description = "Specifies the number of the top distinct lines to be used for gradient score")
        int numberOfBestLines;

        @Parameter(names = {"--nBestSamplesPerLine"},
                description = "Specifies the number of the top distinct samples within a line to be used for gradient score")
        int numberOfBestSamplesPerLine;

        @Parameter(names = {"--nBestMatchesPerSample"},
                description = "Number of best matches for each sample to be used for gradient scoring")
        int numberOfBestMatchesPerSample;

        public GradientScoresArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }
    }

    private final GradientScoresArgs args;
    private final Supplier<Long> cacheSizeSupplier;
    private final ObjectMapper mapper;

    public CalculateGradientScoresCmd(String commandName,
                                      CommonArgs commonArgs,
                                      Supplier<Long> cacheSizeSupplier) {
        super(commandName);
        this.args = new GradientScoresArgs(commonArgs);
        this.cacheSizeSupplier = cacheSizeSupplier;
        this.mapper = new ObjectMapper()
                .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        ;
    }

    @Override
    GradientScoresArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        // initialize the cache
        CachedMIPsUtils.initializeCache(cacheSizeSupplier.get());
        // run gradient scoring
        calculateAllGradientScores();
    }

    private void calculateAllGradientScores() {
        ImageRegionDefinition excludedRegions = args.getRegionGeneratorForTextLabels();
        ColorDepthSearchAlgorithmProvider<ShapeMatchScore> gradScoreAlgorithmProvider = ColorDepthSearchAlgorithmProviderFactory.createShapeMatchCDSAlgorithmProvider(
                args.mirrorMask,
                args.negativeRadius,
                args.borderSize,
                loadQueryROIMask(args.queryROIMaskName),
                excludedRegions
        );
        Executor executor = CmdUtils.createCmdExecutor(args.commonArgs);
        CDMatchesReader<EMNeuronMetadata, LMNeuronMetadata> cdMatchesReader = getCDMatchesReader(args.matches);

        long startTime = System.currentTimeMillis();
        List<String> itemsToProcess = cdMatchesReader.listCDMatchesLocations();
        int size = itemsToProcess.size();
        ItemsHandling.partitionCollection(itemsToProcess, args.processingPartitionSize).stream().parallel()
                .forEach(partititionItems -> {
                    long startProcessingPartitionTime = System.currentTimeMillis();
                    partititionItems.forEach(toProcess -> {
                        List<CDSMatch<EMNeuronMetadata, LMNeuronMetadata>> scoredEmToLmMatches = calculateGradientScores(
                                gradScoreAlgorithmProvider,
                                cdMatchesReader,
                                toProcess,
                                executor
                        );
                        // TODO !!!!!!
                    });
                    LOG.info("Finished a batch of {} in {}s - meory usage {}M out of {}M",
                            partititionItems.size(),
                            (System.currentTimeMillis() - startProcessingPartitionTime) / 1000.,
                            (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1, // round up
                            (Runtime.getRuntime().totalMemory() / _1M));
                });
        LOG.info("Finished calculating gradient scores for {} items in {}s - memory usage {}M out of {}M",
                size,
                (System.currentTimeMillis() - startTime) / 1000.,
                (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / _1M + 1, // round up
                (Runtime.getRuntime().totalMemory() / _1M));
    }

    private ImageArray<?> loadQueryROIMask(String queryROIMask) {
        if (StringUtils.isBlank(queryROIMask)) {
            return null;
        } else {
            return NeuronMIPUtils.loadImageFromFileData(FileData.fromString(queryROIMask));
        }
    }

    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> CDMatchesReader<M, T> getCDMatchesReader(List<ListArg> matchesArg) {
        List<String> filesToProcess = matchesArg.stream()
                .flatMap(arg -> IOUtils.getFiles(arg.input, arg.offset, arg.length).stream())
                .collect(Collectors.toList());
        // for now only handle JSON file input but in the future will handle DB data as well
        return new JSONFileCDMatchesReader<>(filesToProcess, mapper);
    }

    private <M extends AbstractNeuronMetadata, T extends AbstractNeuronMetadata> List<CDSMatch<M, T>> calculateGradientScores(
            ColorDepthSearchAlgorithmProvider<ShapeMatchScore> gradScoreAlgorithmProvider,
            CDMatchesReader<M, T> cdsMatchesReader,
            String cdsMatchesSource,
            Executor executor) {
        List<CDSMatch<M, T>> cdsMatches = cdsMatchesReader.readCDMatches(cdsMatchesSource);
        // select best matches to process

        return cdsMatches;
    }
}
