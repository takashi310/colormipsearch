package org.janelia.colormipsearch.cmd;

import java.util.List;
import java.util.function.Supplier;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import org.janelia.colormipsearch.utils.CachedMIPsUtils;

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

    ColorDepthSearchCmd(String commandName,
                        CommonArgs commonArgs,
                        Supplier<Long> cacheSizeSupplier) {
        super(commandName);
        this.args = new ColorDepthSearchArgs(commonArgs);
        this.cacheSizeSupplier = cacheSizeSupplier;
    }

    @Override
    ColorDepthSearchArgs getArgs() {
        return args;
    }

    @Override
    void execute() {
        // initialize the cache
        CachedMIPsUtils.initializeCache(cacheSizeSupplier.get());
    }

    private void writeResults() {
        // FIXME !!!!!!!
    }
}
