package org.janelia.colormipsearch;

import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

public class ReplaceURLsCommand {

    @Parameters(commandDescription = "Replace image URLs")
    private static class ReplaceURLsArgs {
        @Parameter(names = {"--images", "-i"}, required = true,
                description = "Comma-delimited list of directories containing images to search")
        private String imagesFilename;

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
