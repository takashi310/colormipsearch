package org.janelia.colormipsearch;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

public class ReplaceURLsCommand {

    @Parameters(commandDescription = "Replace image URLs from the source MIPs to the URLs from the target MIPs")
    private static class ReplaceURLsArgs {
        @Parameter(names = {"--source-mips", "-src"}, required = true,
                description = "File containing the MIPS whose image URLs will change")
        private String sourceMIPsFilename;
        @Parameter(names = {"--target-mips", "-target"}, required = true,
                description = "File containing the MIPS with the image URLs")
        private String targetMIPsFilename;

        @ParametersDelegate
        final CommonArgs commonArgs;

        ReplaceURLsArgs(CommonArgs commonArgs) {
            this.commonArgs = commonArgs;
        }
    }

    private final ReplaceURLsArgs args;

    ReplaceURLsCommand(CommonArgs commonArgs) {
        args =  new ReplaceURLsArgs(commonArgs);
    }

    ReplaceURLsArgs getArgs() {
        return args;
    }

    void execute() {
        replaceURLS(args);
    }

    private void replaceURLS(ReplaceURLsArgs args) {
        // TODO
    }

}
