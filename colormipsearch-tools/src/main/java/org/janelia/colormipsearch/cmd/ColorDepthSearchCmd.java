package org.janelia.colormipsearch.cmd;

import java.util.List;
import java.util.function.Supplier;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import org.apache.commons.lang3.StringUtils;
import org.janelia.colormipsearch.cds.ColorDepthSearchAlgorithmProvider;
import org.janelia.colormipsearch.cds.ColorDepthSearchAlgorithmProviderFactory;
import org.janelia.colormipsearch.cds.ColorMIPMatchScore;
import org.janelia.colormipsearch.cds.ColorMIPSearch;
import org.janelia.colormipsearch.cmd.CachedMIPsUtils;
import org.janelia.colormipsearch.cmd.cdsprocess.ColorMIPSearchProcessor;
import org.janelia.colormipsearch.imageprocessing.ImageArray;
import org.janelia.colormipsearch.imageprocessing.ImageRegionDefinition;
import org.janelia.colormipsearch.mips.NeuronMIPUtils;
import org.janelia.colormipsearch.model.FileData;

public class ColorDepthSearchCmd extends AbstractCmd {

    @Parameters(commandDescription = "Color depth search for a batch of MIPs")
    static class ColorDepthSearchArgs extends AbstractColorDepthMatchArgs {

        @Parameter(names = {"--images", "-i"}, required = true, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "Comma-delimited list of JSON configs containing images to search")
        List<ListArg> librariesInputs;

        @Parameter(names = {"--images-index"}, description = "Input image file(s) start index")
        long librariesStartIndex;

        @Parameter(names = {"--images-length"}, description = "Input image file(s) length")
        int librariesLength;

        @Parameter(names = {"--masks", "-m"}, required = true, variableArity = true, converter = ListArg.ListArgConverter.class,
                description = "Image file(s) to use as the search masks")
        List<ListArg> masksInputs;

        @Parameter(names = {"--masks-index"}, description = "Mask file(s) start index")
        long masksStartIndex;

        @Parameter(names = {"--masks-length"}, description = "Mask file(s) length")
        int masksLength;

        public ColorDepthSearchArgs(CommonArgs commonArgs) {
            super(commonArgs);
        }
    }

    private final ColorDepthSearchArgs args;
    private final Supplier<Long> cacheSizeSupplier;
    private final boolean useSpark;

    ColorDepthSearchCmd(String commandName,
                        CommonArgs commonArgs,
                        Supplier<Long> cacheSizeSupplier,
                        boolean useSpark) {
        super(commandName);
        this.args = new ColorDepthSearchArgs(commonArgs);
        this.cacheSizeSupplier = cacheSizeSupplier;
        this.useSpark = useSpark;
    }

    @Override
    ColorDepthSearchArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        // initialize the cache
        CachedMIPsUtils.initializeCache(cacheSizeSupplier.get());

        runColorDepthSearch();
    }

    private void runColorDepthSearch() {
        ColorMIPSearchProcessor<?, ?> colorMIPSearchProcessor;
        ColorDepthSearchAlgorithmProvider<ColorMIPMatchScore> cdsAlgorithmProvider;
        ImageRegionDefinition excludedRegions = args.getRegionGeneratorForTextLabels();
        if (args.onlyPositiveScores()) {
            cdsAlgorithmProvider = ColorDepthSearchAlgorithmProviderFactory.createPixMatchCDSAlgorithmProvider(
                    args.mirrorMask,
                    args.dataThreshold,
                    args.pixColorFluctuation,
                    args.xyShift,
                    excludedRegions
            );
        } else {
            cdsAlgorithmProvider = ColorDepthSearchAlgorithmProviderFactory.createPixMatchWithNegativeScoreCDSAlgorithmProvider(
                    args.mirrorMask,
                    args.dataThreshold,
                    args.pixColorFluctuation,
                    args.xyShift,
                    args.negativeRadius,
                    loadQueryROIMask(args.queryROIMaskName),
                    excludedRegions
            );
        }
        ColorMIPSearch colorMIPSearch = new ColorMIPSearch(args.pctPositivePixels, args.maskThreshold, cdsAlgorithmProvider);
        // !!!! TODO
    }

    private ImageArray<?> loadQueryROIMask(String queryROIMask) {
        if (StringUtils.isBlank(queryROIMask)) {
            return null;
        } else {
            return NeuronMIPUtils.loadImageFromFileData(FileData.fromString(queryROIMask));
        }
    }

    private void writeResults() {
        // FIXME !!!!!!!
    }
}
