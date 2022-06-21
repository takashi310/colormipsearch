package org.janelia.colormipsearch.cmd;

import java.util.function.Supplier;

import com.beust.jcommander.Parameters;

import org.janelia.colormipsearch.utils.CachedMIPsUtils;

public class ColorDepthSearchCmd extends AbstractCmd {

    @Parameters(commandDescription = "Color depth search for a batch of MIPs")
    static class ColorDepthSearchArgs extends AbstractColorDepthMatchArgs {

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
}
